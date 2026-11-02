import { useEffect, useRef, useState } from 'react'

const SIZE = 200 // NM, must match the gateway's airspace
const PX = 640

// Receives the gateway's picture over a WebSocket, reconnecting on drop.
function usePicture() {
  const [pic, setPic] = useState(null)
  const [connected, setConnected] = useState(false)
  useEffect(() => {
    let ws, timer, closed = false
    const open = () => {
      const proto = location.protocol === 'https:' ? 'wss' : 'ws'
      ws = new WebSocket(`${proto}://${location.host}/ws`)
      ws.onopen = () => setConnected(true)
      ws.onmessage = (e) => setPic(JSON.parse(e.data))
      ws.onclose = () => {
        setConnected(false)
        if (!closed) timer = setTimeout(open, 1000)
      }
    }
    open()
    return () => { closed = true; clearTimeout(timer); ws && ws.close() }
  }, [])
  return [pic, connected]
}

function Radar({ pic, selected, onSelect }) {
  const ref = useRef(null)
  useEffect(() => {
    const ctx = ref.current.getContext('2d')
    const s = PX / SIZE
    const toY = (y) => PX - y * s // north up
    ctx.clearRect(0, 0, PX, PX)

    ctx.strokeStyle = '#1f5a3a'
    ctx.fillStyle = '#3c8a5c'
    ctx.font = '12px monospace'
    for (const sec of pic?.sectors ?? []) {
      const [x, y, w, h] = sec.box
      ctx.strokeRect(x * s, toY(y + h), w * s, h * s)
      ctx.fillText(sec.name, x * s + 6, toY(y + h) + 16)
    }
    ctx.beginPath() // tower circle, matches the gateway's 15 NM radius
    ctx.arc(100 * s, toY(100), 15 * s, 0, 2 * Math.PI)
    ctx.stroke()

    for (const ac of pic?.aircraft ?? []) {
      const x = ac.x * s, y = toY(ac.y)
      // amber while the registry says TRANSFERRING, otherwise green
      const color = ac.status === 'TRANSFERRING' ? '#ffb000' : '#b8f5c8'
      ctx.fillStyle = color
      ctx.fillRect(x - 2, y - 2, 4, 4)
      const r = (ac.heading * Math.PI) / 180 // velocity leader line
      ctx.strokeStyle = color
      ctx.beginPath(); ctx.moveTo(x, y); ctx.lineTo(x + 10 * Math.sin(r), y - 10 * Math.cos(r)); ctx.stroke()
      ctx.fillStyle = '#7fd69a'
      ctx.fillText(`${ac.callsign} ${Math.round(ac.alt / 100)}`, x + 5, y - 5)
      if (ac.callsign === selected) { // ring around the aircraft being cleared
        ctx.strokeStyle = '#ffffff'
        ctx.beginPath(); ctx.arc(x, y, 9, 0, 2 * Math.PI); ctx.stroke()
      }
      ctx.strokeStyle = '#1f5a3a'
    }
  }, [pic, selected])

  // Click selects the nearest aircraft within 12 px. The canvas can be shown
  // smaller than PX (max-width: 100%), so scale the click back to canvas pixels.
  const click = (e) => {
    const rect = ref.current.getBoundingClientRect()
    const cx = ((e.clientX - rect.left) * PX) / rect.width
    const cy = ((e.clientY - rect.top) * PX) / rect.height
    const s = PX / SIZE
    let best = null, bestD = 12
    for (const ac of pic?.aircraft ?? []) {
      const d = Math.hypot(ac.x * s - cx, PX - ac.y * s - cy)
      if (d < bestD) { best = ac.callsign; bestD = d }
    }
    onSelect(best)
  }
  return <canvas ref={ref} width={PX} height={PX} onClick={click} style={{ cursor: 'crosshair' }} />
}

const PRESETS = ['CLIMB FL350', 'DESCEND FL250', 'HEADING 090', 'HEADING 270', 'REDUCE SPEED 250']

// Clearance form for the selected aircraft. Login comes in the next step;
// until then the controller name is just a text field.
function ClearanceForm({ pic, selected }) {
  const [instruction, setInstruction] = useState('')
  const [issuedBy, setIssuedBy] = useState('ctrl-demo')
  const [result, setResult] = useState(null)
  const ac = (pic?.aircraft ?? []).find((a) => a.callsign === selected)

  const send = async (text) => {
    setResult({ status: 'sending' })
    try {
      const res = await fetch('/api/clearance', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ callsign: selected, instruction: text, issued_by: issuedBy }),
      })
      setResult(await res.json())
    } catch (err) {
      setResult({ status: 'rejected', reason: `gateway unreachable: ${err.message}` })
    }
  }

  if (!selected) return <div className="hint">Click an aircraft on the radar to issue a clearance.</div>
  return (
    <div className="clearance-form">
      <h2>clearance · {selected}</h2>
      {ac
        ? <div className="hint">owner {ac.owner || '-'} · {ac.status || 'unclaimed'} · FL{Math.round(ac.alt / 100)} · hdg {Math.round(ac.heading)}</div>
        : <div className="hint">aircraft no longer on the radar</div>}
      <div className="row">
        <input value={instruction} onChange={(e) => setInstruction(e.target.value)}
          onKeyDown={(e) => e.key === 'Enter' && instruction.trim() && send(instruction)}
          placeholder="e.g. DESCEND FL250" maxLength={64} />
        <button onClick={() => send(instruction)} disabled={!instruction.trim()}>Send</button>
      </div>
      <div className="row presets">
        {PRESETS.map((p) => <button key={p} onClick={() => send(p)}>{p}</button>)}
      </div>
      <div className="row">
        <label>controller <input value={issuedBy} onChange={(e) => setIssuedBy(e.target.value)} /></label>
      </div>
      {result && <div className={`result ${result.status}`}>{describe(result)}</div>}
    </div>
  )
}

function describe(c) {
  if (c.error) return `rejected: ${c.error}`
  switch (c.status) {
    case 'sending': return 'sending...'
    case 'committed': return `committed in ${c.sector} at log index ${c.index}`
    case 'pending': return `pending: ${c.reason} (waiting on ${c.waiting_on})`
    default: return `${c.status}: ${c.reason ?? ''}`
  }
}

// Recent clearances from the gateway; buffered ones update live.
function Clearances({ pic }) {
  const list = pic?.clearances ?? []
  const st = pic?.clearance_stats
  const queued = Object.entries(pic?.clearance_queues ?? {})
  return (
    <div>
      <h2>clearances</h2>
      {st && <div className="hint">
        {st.committed} committed · {st.rejected} rejected · {st.expired} expired · {st.buffered} ever buffered
        {queued.length > 0 && ` · buffered now: ${queued.map(([c, n]) => `${c} ${n}`).join(', ')}`}
      </div>}
      <table><tbody>
        {list.slice(0, 10).map((c) => (
          <tr key={c.id} className={c.status}>
            <td>{c.callsign}</td><td>{c.instruction}</td>
            <td>{c.status}{c.index ? ` #${c.index}` : ''}</td>
            <td>{c.sector ?? ''}</td>
          </tr>
        ))}
      </tbody></table>
    </div>
  )
}

function Clusters({ pic }) {
  const names = Object.keys(pic?.clusters ?? {}).sort()
  return names.map((c) => (
    <div key={c}>
      <h2>{c} <ReadyTag r={pic.ready?.[c]} /></h2>
      <table><tbody>
        {pic.clusters[c].map((n) => (
          <tr key={n.id} className={n.up ? n.state : 'down'}>
            <td>{n.id}</td><td>{n.up ? n.state : 'DOWN'}</td><td>term {n.term ?? '-'}</td><td>commit {n.commit_index ?? '-'}</td>
          </tr>
        ))}
      </tbody></table>
    </div>
  ))
}

// Gateway's Ready flag for one cluster: one leader, recently polled.
function ReadyTag({ r }) {
  if (!r) return null
  if (r.ready) return <span className="ready">READY</span>
  const secs = r.since_ms ? ((Date.now() - r.since_ms) / 1000).toFixed(1) : null
  return <span className="notready">NOT READY ({r.reason}{secs ? `, ${secs}s` : ''})</span>
}

function Handoffs({ pic }) {
  const h = pic?.handoff
  if (!h) return null
  const moving = (pic.aircraft ?? []).filter((a) => a.status === 'TRANSFERRING').length
  return (
    <div>
      <h2>handoffs (registry, 3-phase)</h2>
      <table><tbody>
        <tr><td>completed</td><td>{h.handoffs}</td><td>avg {h.avg_ms?.toFixed(1)} ms</td></tr>
        <tr><td>in progress</td><td>{moving}</td><td>queued {h.queued}</td></tr>
        <tr><td>aborted</td><td>{h.aborted}</td><td>recovered {h.recovered}</td></tr>
        <tr><td>deferred</td><td>{h.deferred ?? 0}</td><td>target not ready</td></tr>
        <tr><td>multicast</td><td>{h.multicast_batches} sent</td><td>{h.multicast_dropped} dropped</td></tr>
      </tbody></table>
    </div>
  )
}

export default function App() {
  const [pic, connected] = usePicture()
  const [selected, setSelected] = useState(null)
  return (
    <div className="layout">
      <Radar pic={pic} selected={selected} onSelect={setSelected} />
      <div className="side">
        <h1>Air Traffic Controller but Different</h1>
        <div className="status">{connected ? 'live' : 'connecting to gateway...'} · {pic?.aircraft?.length ?? 0} aircraft</div>
        <ClearanceForm pic={pic} selected={selected} />
        <Clearances pic={pic} />
        <Handoffs pic={pic} />
        {/* TODO: live safety-invariant panel and spoofed-aircraft alerts go here */}
        <Clusters pic={pic} />
      </div>
    </div>
  )
}
