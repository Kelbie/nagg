package appview

import "net/http"

// MintObservatoryHandler serves the ecosystem-changelog page: a self-contained
// HTML page (no external assets) that fetches /nostr/mint/changes client-side
// and renders the global feed of mint /v1/info revisions. Mounted at
// /mint-changes. Same-origin, so no CORS; the JSON endpoint carries the data.
func MintObservatoryHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "GET /mint-changes only", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(observatoryHTML))
	}
}

const observatoryHTML = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Mint Observatory — nagg</title>
<style>
  :root {
    --ground:#F5F8F6; --surface:#FFFFFF; --surface-2:#EFF3F1; --ink:#131C18; --ink-soft:#34433D;
    --muted:#5C6B64; --faint:#86938C; --line:#DCE3DF; --line-strong:#C4CEC8; --accent:#0C7C6C;
    --accent-ink:#075E52; --accent-wash:#E2F0EC; --add:#2C8B4B; --remove:#C0433D; --change:#B07C22;
    --mono:ui-monospace,"SF Mono","JetBrains Mono",Menlo,Consolas,monospace;
    --sans:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;
  }
  @media (prefers-color-scheme: dark) {
    :root{--ground:#0C1210;--surface:#121B17;--surface-2:#16211C;--ink:#E7EDEA;--ink-soft:#C2CDC7;
    --muted:#93A29B;--faint:#6C7A73;--line:#223029;--line-strong:#2E3F37;--accent:#41C2AC;
    --accent-ink:#6FD6C4;--accent-wash:#10261F;--add:#56C878;--remove:#E7756E;--change:#E0AE52;}
  }
  :root[data-theme="light"]{--ground:#F5F8F6;--surface:#FFFFFF;--surface-2:#EFF3F1;--ink:#131C18;--ink-soft:#34433D;
    --muted:#5C6B64;--faint:#86938C;--line:#DCE3DF;--line-strong:#C4CEC8;--accent:#0C7C6C;--accent-ink:#075E52;
    --accent-wash:#E2F0EC;--add:#2C8B4B;--remove:#C0433D;--change:#B07C22;}
  :root[data-theme="dark"]{--ground:#0C1210;--surface:#121B17;--surface-2:#16211C;--ink:#E7EDEA;--ink-soft:#C2CDC7;
    --muted:#93A29B;--faint:#6C7A73;--line:#223029;--line-strong:#2E3F37;--accent:#41C2AC;--accent-ink:#6FD6C4;
    --accent-wash:#10261F;--add:#56C878;--remove:#E7756E;--change:#E0AE52;}

  *{box-sizing:border-box}
  body{margin:0;background:var(--ground);color:var(--ink);font-family:var(--sans);
    font-size:15px;line-height:1.55;-webkit-font-smoothing:antialiased}
  .wrap{max-width:860px;margin:0 auto;padding:0 20px 80px}
  a{color:var(--accent-ink);text-decoration:none;border-bottom:1px solid var(--line-strong)}
  a:hover{border-color:var(--accent)}

  header{padding:44px 0 22px;border-bottom:1px solid var(--line);margin-bottom:8px}
  .brand{font-family:var(--mono);font-size:12px;letter-spacing:.18em;text-transform:uppercase;
    color:var(--accent-ink);display:flex;align-items:center;gap:9px}
  .brand .dot{width:6px;height:6px;border-radius:50%;background:var(--accent);display:inline-block}
  .brand .live{margin-left:auto;font-size:10.5px;color:var(--faint);letter-spacing:.1em;display:flex;align-items:center;gap:6px}
  .brand .live i{width:6px;height:6px;border-radius:50%;background:var(--add);display:inline-block;animation:pulse 2.4s ease-in-out infinite}
  @keyframes pulse{0%,100%{opacity:.35}50%{opacity:1}}
  @media (prefers-reduced-motion: reduce){.brand .live i{animation:none}}
  h1{font-family:var(--mono);font-weight:600;letter-spacing:-.02em;font-size:clamp(26px,5vw,38px);
    margin:16px 0 10px;line-height:1.1;text-wrap:balance}
  .sub{color:var(--ink-soft);margin:0;max-width:640px}
  .stats{display:flex;flex-wrap:wrap;gap:8px;margin-top:22px}
  .stat{font-family:var(--mono);font-size:12.5px;border:1px solid var(--line-strong);border-radius:999px;
    padding:5px 13px;background:var(--surface);color:var(--muted)}
  .stat b{color:var(--ink);font-weight:600;font-variant-numeric:tabular-nums}
  .toolbar{display:flex;align-items:center;gap:12px;margin:18px 0 4px;font-family:var(--mono);font-size:12px;color:var(--faint)}
  .toolbar .spacer{flex:1}
  button{font-family:var(--mono);font-size:12px;color:var(--ink-soft);background:var(--surface);
    border:1px solid var(--line-strong);border-radius:7px;padding:5px 11px;cursor:pointer}
  button:hover{border-color:var(--accent);color:var(--accent-ink)}

  .feed{margin-top:16px;display:flex;flex-direction:column;gap:14px}
  .change{background:var(--surface);border:1px solid var(--line);border-radius:12px;padding:16px 18px;
    box-shadow:0 1px 2px rgba(19,28,24,.04)}
  .change .head{display:flex;align-items:baseline;gap:10px;flex-wrap:wrap}
  .change .name{font-family:var(--mono);font-weight:600;font-size:14.5px;color:var(--ink)}
  .change .when{font-family:var(--mono);font-size:12px;color:var(--faint);margin-left:auto;white-space:nowrap;font-variant-numeric:tabular-nums}
  .change .url{font-family:var(--mono);font-size:11.5px;color:var(--muted);margin:2px 0 12px;word-break:break-all}
  .sumlist{list-style:none;margin:0;padding:0;display:flex;flex-direction:column;gap:6px}
  .sumlist li{display:flex;gap:9px;align-items:baseline;font-size:14px}
  .sumlist li .mk{font-family:var(--mono);font-size:12px;color:var(--change);flex:none;width:14px;text-align:center}
  .sumlist li.add .mk{color:var(--add)} .sumlist li.rm .mk{color:var(--remove)}
  details{margin-top:12px}
  summary{font-family:var(--mono);font-size:11px;letter-spacing:.06em;text-transform:uppercase;
    color:var(--faint);cursor:pointer;list-style:none}
  summary::-webkit-details-marker{display:none}
  summary:before{content:"▸ ";color:var(--accent)}
  details[open] summary:before{content:"▾ "}
  pre{margin:10px 0 0;background:var(--surface-2);border:1px solid var(--line);border-radius:8px;
    padding:12px;overflow-x:auto;font-family:var(--mono);font-size:12px;line-height:1.5;color:var(--ink-soft)}
  .empty{text-align:center;padding:64px 20px;color:var(--muted)}
  .empty .big{font-family:var(--mono);font-size:16px;color:var(--ink);margin-bottom:10px}
  .empty .hint{max-width:460px;margin:0 auto;font-size:14px;line-height:1.6}
  footer{margin-top:40px;padding-top:16px;border-top:1px solid var(--line);
    font-family:var(--mono);font-size:12px;color:var(--faint);display:flex;gap:16px;flex-wrap:wrap}
  :focus-visible{outline:2px solid var(--accent);outline-offset:2px;border-radius:4px}
</style>
</head>
<body>
<div class="wrap">
  <header>
    <div class="brand"><span class="dot"></span> nagg · mint observatory
      <span class="live"><i></i> LIVE</span>
    </div>
    <h1>Cashu ecosystem info changes</h1>
    <p class="sub">Every tracked mint's NUT-06 <code>/v1/info</code>, snapshotted daily and diffed over time — new software versions, added NUTs, changed contacts, message-of-the-day. Newest first.</p>
    <div class="stats" id="stats"></div>
  </header>

  <div class="toolbar">
    <span id="updated">loading…</span>
    <span class="spacer"></span>
    <button id="refresh" type="button">Refresh</button>
    <button id="theme" type="button">Theme</button>
  </div>

  <main class="feed" id="feed"></main>

  <footer>
    <a href="/nostr/mint/changes">JSON</a>
    <a href="/graphiql">GraphQL</a>
    <span>auto-refreshes every 60s</span>
  </footer>
</div>

<script>
(function(){
  var feed=document.getElementById("feed");
  var statsEl=document.getElementById("stats");
  var updatedEl=document.getElementById("updated");

  function esc(s){return String(s==null?"":s).replace(/[&<>"']/g,function(c){
    return {"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;"}[c];});}

  function rel(sec){
    var d=Math.floor(Date.now()/1000)-sec;
    if(d<60)return "just now";
    if(d<3600)return Math.floor(d/60)+"m ago";
    if(d<86400)return Math.floor(d/3600)+"h ago";
    if(d<2592000)return Math.floor(d/86400)+"d ago";
    return Math.floor(d/2592000)+"mo ago";
  }
  function abs(sec){return new Date(sec*1000).toISOString().replace("T"," ").slice(0,16)+" UTC";}

  function marker(line){
    var l=line.toLowerCase();
    if(l.indexOf("enabled")>=0||l.indexOf("added")>=0)return {cls:"add",mk:"+"};
    if(l.indexOf("removed")>=0||l.indexOf("disabled")>=0)return {cls:"rm",mk:"−"};
    return {cls:"",mk:"~"};
  }

  function renderChange(c){
    var lines=(c.summary&&c.summary.length?c.summary:["configuration changed"]).map(function(s){
      var m=marker(s);
      return '<li class="'+m.cls+'"><span class="mk">'+m.mk+'</span><span>'+esc(s)+'</span></li>';
    }).join("");
    var name=c.name&&c.name.trim()?c.name:c.mintUrl;
    var patch="";
    if(c.patch){
      patch='<details><summary>raw diff (RFC 6902)</summary><pre>'+esc(JSON.stringify(c.patch,null,2))+'</pre></details>';
    }
    return '<article class="change">'+
      '<div class="head"><span class="name">'+esc(name)+'</span>'+
      '<span class="when" title="'+esc(abs(c.at))+'">'+esc(rel(c.at))+'</span></div>'+
      '<div class="url">'+esc(c.mintUrl)+'</div>'+
      '<ul class="sumlist">'+lines+'</ul>'+patch+'</article>';
  }

  function renderEmpty(d){
    return '<div class="empty"><div class="big">No changes recorded yet</div>'+
      '<p class="hint">Watching <b>'+(d.trackedMints||0)+'</b> mints. A change needs two different snapshots of a mint\'s info, so the first entries appear once a tracked mint updates its <code>/v1/info</code>. Check back — this streams in as the ecosystem moves.</p></div>';
  }

  function renderStats(d){
    statsEl.innerHTML=
      '<span class="stat"><b>'+(d.trackedMints||0)+'</b> mints tracked</span>'+
      '<span class="stat"><b>'+(d.reachableMints||0)+'</b> reachable</span>'+
      '<span class="stat"><b>'+(d.totalChanges||0)+'</b> changes recorded</span>';
  }

  function load(){
    updatedEl.textContent="refreshing…";
    fetch("/nostr/mint/changes?limit=200",{headers:{"accept":"application/json"}})
      .then(function(r){if(!r.ok)throw new Error("HTTP "+r.status);return r.json();})
      .then(function(d){
        renderStats(d);
        if(!d.changes||d.changes.length===0){feed.innerHTML=renderEmpty(d);}
        else{feed.innerHTML=d.changes.map(renderChange).join("");}
        updatedEl.textContent="updated "+new Date().toLocaleTimeString();
      })
      .catch(function(e){
        feed.innerHTML='<div class="empty"><div class="big">Couldn\'t load changes</div><p class="hint">'+esc(e.message)+'</p></div>';
        updatedEl.textContent="error";
      });
  }

  document.getElementById("refresh").addEventListener("click",load);
  document.getElementById("theme").addEventListener("click",function(){
    var cur=document.documentElement.getAttribute("data-theme");
    var next=cur==="dark"?"light":(cur==="light"?"dark":(matchMedia("(prefers-color-scheme: dark)").matches?"light":"dark"));
    document.documentElement.setAttribute("data-theme",next);
  });

  load();
  setInterval(load,60000);
})();
</script>
</body>
</html>`
