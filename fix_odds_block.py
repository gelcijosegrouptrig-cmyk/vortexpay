#!/usr/bin/env python3
"""Substitui bloco de odds no home.html"""

with open('/home/user/vortex_deploy/home.html', 'r', encoding='utf-8') as f:
    content = f.read()

start_marker = "predHtml +\n        '<div class=\"jc-odds'"
end_marker = "        '</div>';\n      lista.appendChild(card);"

start_idx = content.find(start_marker)
end_idx = content.find(end_marker, start_idx)

if start_idx == -1 or end_idx == -1:
    print(f"ERR marcadores: start={start_idx}, end={end_idx}")
    exit(1)

real_start = content.rfind('\n', 0, start_idx) + 1
end_idx_full = end_idx + len(end_marker)

print(f"Trecho a substituir ({end_idx_full - real_start} chars)")

pin_emoji = '\U0001f4cd'
lock_emoji = '\U0001f512'
cdot = '\u00b7'

new_chunk = (
    "        predHtml +\n"
    "        (j.venue ? '<div style=\"font-size:10px;color:var(--muted);padding:0 12px 4px;display:flex;align-items:center;gap:4px\">" + pin_emoji + " ' + j.venue + (j.round ? ' " + cdot + " R'+j.round : '') + '</div>' : '') +\n"
    "        (o.home!=null || o.away!=null\n"
    "          ? '<div class=\"jc-odds' + (temEmpate ? '' : ' no-draw') + '\">' +\n"
    "              (o.home!=null ? '<div class=\"odd-btn2\" id=\"ob-'+j.id+'-c\" onclick=\"selOdd(\\''+j.id+'\\',\\''+j.home_team+'\\',\\'C\\','+o.home+',\\''+j.home_team+' x '+j.away_team+'\\')\">"
    "<div class=\"o-lbl\">'+teamShortLabel(j.home_team)+'</div><div class=\"o-val\">'+Number(o.home).toFixed(2)+'</div></div>' : '') +\n"
    "              (temEmpate ? '<div class=\"odd-btn2\" id=\"ob-'+j.id+'-e\" onclick=\"selOdd(\\''+j.id+'\\',\\'Empate\\',\\'X\\','+o.draw+',\\''+j.home_team+' x '+j.away_team+'\\')\">"
    "<div class=\"o-lbl\">Empate</div><div class=\"o-val\">'+Number(o.draw).toFixed(2)+'</div></div>' : '') +\n"
    "              (o.away!=null ? '<div class=\"odd-btn2\" id=\"ob-'+j.id+'-f\" onclick=\"selOdd(\\''+j.id+'\\',\\''+j.away_team+'\\',\\'F\\','+o.away+',\\''+j.home_team+' x '+j.away_team+'\\')\">"
    "<div class=\"o-lbl\">'+teamShortLabel(j.away_team)+'</div><div class=\"o-val\">'+Number(o.away).toFixed(2)+'</div></div>' : '') +\n"
    "            '</div>'\n"
    "          : '<div style=\"padding:8px 12px;text-align:center;font-size:11px;color:var(--muted)\">" + lock_emoji + " Odds em breve</div>'\n"
    "        );\n"
    "      lista.appendChild(card);"
)

content = content[:real_start] + new_chunk + content[end_idx_full:]

with open('/home/user/vortex_deploy/home.html', 'w', encoding='utf-8') as f:
    f.write(content)

print(f"OK! File: {len(content)} chars")

idx2 = content.find("predHtml +")
print("Verificacao:", repr(content[idx2:idx2+150]))
