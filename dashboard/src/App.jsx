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

// Aircraft colour: red for SUSPECT (failed the receiver cross-check), amber
// while TRANSFERRING, grey while UNVERIFIED (not enough receiver coverage
// yet), otherwise green.
function aircraftColor(ac) {
  if (ac.verify === 'SUSPECT') return '#ff4d4d'
  if (ac.status === 'TRANSFERRING') return '#ffb000'
  if (ac.verify === 'UNVERIFIED') return '#8a9a90'
  return '#b8f5c8'
}

function Radar({ pic, selected, onSelect, coverage }) {
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

    // Ground receivers: triangles, grey when the gateway stopped hearing them.
    for (const rx of pic?.receivers ?? []) {
      const x = rx.x * s, y = toY(rx.y)
      const color = rx.up ? '#4fc3f7' : '#666'
      if (coverage) {
        ctx.strokeStyle = rx.up ? 'rgba(79,195,247,0.25)' : 'rgba(120,120,120,0.2)'
        ctx.setLineDash([4, 4])
        ctx.beginPath(); ctx.arc(x, y, rx.range * s, 0, 2 * Math.PI); ctx.stroke()
        ctx.setLineDash([])
      }
      ctx.fillStyle = color
      ctx.beginPath(); ctx.moveTo(x, y - 7); ctx.lineTo(x - 6, y + 5); ctx.lineTo(x + 6, y + 5); ctx.closePath(); ctx.fill()
      const label = rx.up ? rx.id : `${rx.id} DOWN`
      const w = ctx.measureText(label).width
      ctx.fillText(label, x + 8 + w > PX ? x - 8 - w : x + 8, y + 4) // keep labels on the canvas
    }

    for (const ac of pic?.aircraft ?? []) {
      const x = ac.x * s, y = toY(ac.y)
      const color = aircraftColor(ac)
      ctx.fillStyle = color
      ctx.fillRect(x - 2, y - 2, 4, 4)
      const r = (ac.heading * Math.PI) / 180 // velocity leader line
      ctx.strokeStyle = color
      ctx.beginPath(); ctx.moveTo(x, y); ctx.lineTo(x + 10 * Math.sin(r), y - 10 * Math.cos(r)); ctx.stroke()
      ctx.fillStyle = ac.verify === 'SUSPECT' ? '#ff4d4d' : '#7fd69a'
      ctx.fillText(`${ac.callsign}${ac.verify === 'SUSPECT' ? '?' : ''} ${Math.round(ac.alt / 100)}`, x + 5, y - 5)
      if (ac.callsign === selected) { // ring around the aircraft being cleared
        ctx.strokeStyle = '#ffffff'
        ctx.beginPath(); ctx.arc(x, y, 9, 0, 2 * Math.PI); ctx.stroke()
      }
      ctx.strokeStyle = '#1f5a3a'
    }
  }, [pic, selected, coverage])

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

// Controller login. The token lives only in React state (not localStorage),
// so reloading the page logs out; fine for a demo, and nothing for another
// script on the page to steal later.
function Login({ pic, session, setSession }) {
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [error, setError] = useState(null)
  if (!pic?.auth) return null // gateway runs with AUTH=off

  const sec = pic.security
  const counters = sec && <div className="hint">
    {sec.logins} logins · {sec.failed_logins} failed · {sec.unauthorized} refused (no login) · {sec.forbidden} refused (wrong sector)
  </div>

  if (session) {
    const mins = Math.max(0, Math.round((session.expires_at_ms - (pic.time ?? Date.now())) / 60000))
    return (
      <div className="login">
        <h2>controller</h2>
        <div className="row">
          <span>{session.username} · {session.role} · {session.role === 'supervisor' ? 'all sectors' : session.sectors.join(', ')} · {mins} min left</span>
          <button onClick={() => setSession(null)}>Log out</button>
        </div>
        {counters}
      </div>
    )
  }

  const submit = async (e) => {
    e.preventDefault()
    setError(null)
    try {
      const res = await fetch('/api/login', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username, password }),
      })
      const body = await res.json()
      if (!res.ok) { setError(body.error ?? `login failed (${res.status})`); return }
      setPassword('')
      setSession(body)
    } catch (err) {
      setError(`gateway unreachable: ${err.message}`)
    }
  }
  return (
    <form className="login" onSubmit={submit}>
      <h2>controller login</h2>
      <div className="row">
        <input value={username} onChange={(e) => setUsername(e.target.value)} placeholder="username" autoComplete="username" />
        <input type="password" value={password} onChange={(e) => setPassword(e.target.value)} placeholder="password" autoComplete="current-password" />
        <button type="submit" disabled={!username || !password}>Log in</button>
      </div>
      {error && <div className="result rejected">{error}</div>}
      {counters}
    </form>
  )
}

// Why the logged-in controller cannot clear this aircraft, or null. The
// gateway enforces all of these anyway; this only says why up front.
function blockedReason(pic, ac, session) {
  if (ac?.verify === 'SUSPECT') return 'failed position verification (SUSPECT)'
  if (!pic?.auth) return null
  if (!session) return 'log in to issue clearances'
  if (session.role !== 'supervisor' && ac?.owner && !session.sectors.includes(ac.owner)) {
    return `controlled by ${ac.owner}, not one of your sectors`
  }
  return null
}

// Clearance form for the selected aircraft.
function ClearanceForm({ pic, selected, session, setSession }) {
  const [instruction, setInstruction] = useState('')
  const [issuedBy, setIssuedBy] = useState('ctrl-demo') // only used while login is off
  const [result, setResult] = useState(null)
  const ac = (pic?.aircraft ?? []).find((a) => a.callsign === selected)

  const send = async (text) => {
    setResult({ status: 'sending' })
    const headers = { 'Content-Type': 'application/json' }
    if (session) headers.Authorization = `Bearer ${session.token}`
    try {
      const res = await fetch('/api/clearance', {
        method: 'POST',
        headers,
        body: JSON.stringify({ callsign: selected, instruction: text, issued_by: issuedBy }),
      })
      if (res.status === 401) setSession(null) // token expired or gateway restarted: log in again
      setResult(await res.json())
    } catch (err) {
      setResult({ status: 'rejected', reason: `gateway unreachable: ${err.message}` })
    }
  }

  if (!selected) return <div className="hint">Click an aircraft on the radar to issue a clearance.</div>
  const blocked = blockedReason(pic, ac, session)
  return (
    <div className="clearance-form">
      <h2>clearance · {selected}</h2>
      {ac
        ? <div className="hint">owner {ac.owner || '-'} · {ac.status || 'unclaimed'} · FL{Math.round(ac.alt / 100)} · hdg {Math.round(ac.heading)}{ac.verify ? ` · ${ac.verify}` : ''}</div>
        : <div className="hint">aircraft no longer on the radar</div>}
      {blocked && <div className="result rejected">clearances blocked: {blocked}</div>}
      <div className="row">
        <input value={instruction} onChange={(e) => setInstruction(e.target.value)}
          onKeyDown={(e) => e.key === 'Enter' && instruction.trim() && send(instruction)}
          placeholder="e.g. DESCEND FL250" maxLength={64} />
        <button onClick={() => send(instruction)} disabled={!instruction.trim() || !!blocked}>Send</button>
      </div>
      <div className="row presets">
        {PRESETS.map((p) => <button key={p} onClick={() => send(p)} disabled={!!blocked}>{p}</button>)}
      </div>
      {!pic?.auth && <div className="row">
        <label>controller <input value={issuedBy} onChange={(e) => setIssuedBy(e.target.value)} /></label>
      </div>}
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

// Spoofed-aircraft alerts: aircraft whose claimed position disagrees with
// the ranges the ground receivers measured (gateway verify.go).
function Alerts({ pic, coverage, setCoverage, onSelect }) {
  const v = pic?.verify
  if (!v) return null // verification off (no receivers configured)
  const alerts = pic.alerts ?? []
  const counts = { VERIFIED: 0, UNVERIFIED: 0, SUSPECT: 0 }
  for (const a of pic.aircraft ?? []) counts[a.verify] = (counts[a.verify] ?? 0) + 1
  const rxs = pic.receivers ?? []
  return (
    <div>
      <h2>position verification {alerts.length > 0 && <span className="notready">{alerts.length} SUSPECT</span>}</h2>
      <div className="hint">
        {counts.VERIFIED} verified · {counts.UNVERIFIED} unverified · {counts.SUSPECT} suspect ·
        receivers {rxs.filter((r) => r.up).length}/{rxs.length} up ·{' '}
        <label><input type="checkbox" checked={coverage} onChange={(e) => setCoverage(e.target.checked)} /> coverage</label>
      </div>
      {alerts.length > 0 && <table><tbody>
        {alerts.map((a) => (
          <tr key={a.callsign} className="rejected" onClick={() => onSelect(a.callsign)} style={{ cursor: 'pointer' }}>
            <td>{a.callsign}?</td>
            <td>{a.reason}</td>
            <td>{((Date.now() - a.since_ms) / 1000).toFixed(0)}s</td>
          </tr>
        ))}
      </tbody></table>}
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
  const [coverage, setCoverage] = useState(false)
  const [session, setSession] = useState(null) // login response: token, username, role, sectors
  // Drop the session once the token has expired, rather than waiting for a 401.
  useEffect(() => {
    if (session && pic?.time > session.expires_at_ms) setSession(null)
  }, [pic, session])
  return (
    <div className="layout">
      <Radar pic={pic} selected={selected} onSelect={setSelected} coverage={coverage} />
      <div className="side">
        <h1>Air Traffic Controller but Different</h1>
        <div className="status">{connected ? 'live' : 'connecting to gateway...'} · {pic?.aircraft?.length ?? 0} aircraft</div>
        <Login pic={pic} session={session} setSession={setSession} />
        <ClearanceForm pic={pic} selected={selected} session={session} setSession={setSession} />
        <Alerts pic={pic} coverage={coverage} setCoverage={setCoverage} onSelect={setSelected} />
        <Clearances pic={pic} />
        <Handoffs pic={pic} />
        {/* TODO: live safety-invariant panel goes here */}
        <Clusters pic={pic} />
      </div>
    </div>
  )
}
