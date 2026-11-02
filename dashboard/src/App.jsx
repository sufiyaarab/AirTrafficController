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

function Radar({ pic }) {
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
      ctx.strokeStyle = '#1f5a3a'
    }
  }, [pic])
  return <canvas ref={ref} width={PX} height={PX} />
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
  return (
    <div className="layout">
      <Radar pic={pic} />
      <div className="side">
        <h1>Air Traffic Controller but Different</h1>
        <div className="status">{connected ? 'live' : 'connecting to gateway...'} · {pic?.aircraft?.length ?? 0} aircraft</div>
        <Handoffs pic={pic} />
        {/* TODO: live safety-invariant panel and spoofed-aircraft alerts go here */}
        <Clusters pic={pic} />
      </div>
    </div>
  )
}
