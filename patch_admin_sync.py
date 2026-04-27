#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Patch admin.html — sincronizar com últimas atualizações:
1. Adicionar card "Ganhadores Recentes" + botão exportar na tab-sorteio
2. Adicionar endpoints novos na documentação
3. Adicionar coluna Data na tabela de apostas recentes
4. Adicionar função carregarGanhadoresAdmin()
"""

def main():
    with open('admin.html', 'r', encoding='utf-8') as f:
        content = f.read()

    errors = []

    # ══════════════════════════════════════════════════════════
    # 1. Card Ganhadores Recentes na tab-sorteio
    # Inserir ANTES do fechamento do tab-sorteio (após historico)
    # ══════════════════════════════════════════════════════════
    OLD_HIST_END = """        <!-- Histórico de sorteios -->
        <div class=\"table-card\">
          <div class=\"table-header\">
            <h3>📋 Histórico de Sorteios</h3>
          </div>
          <div style=\"overflow-x:auto\">
            <table class=\"data-table\">
              <thead><tr>
                <th>Data</th><th>Ganhador</th><th>Nº Sorte</th>
                <th>Prêmio</th><th>Participantes</th><th>Total Saldo</th>
              </tr></thead>
              <tbody id=\"historico-sorteio-tbody\">
                <tr><td colspan=\"6\" style=\"text-align:center;color:#666;padding:20px\">Carregando...</td></tr>
              </tbody>
            </table>
          </div>
        </div>

      </div><!-- /tab-sorteio -->"""

    NEW_HIST_END = """        <!-- Ganhadores Recentes -->
        <div class=\"table-card\" style=\"margin-bottom:20px\">
          <div class=\"table-header\" style=\"display:flex;justify-content:space-between;align-items:center\">
            <h3>🏆 Ganhadores Recentes</h3>
            <button onclick=\"carregarGanhadoresAdmin()\" class=\"action-btn\" style=\"font-size:12px;padding:6px 12px\">🔄 Atualizar</button>
          </div>
          <div style=\"overflow-x:auto\">
            <table class=\"data-table\">
              <thead><tr>
                <th>Data</th><th>Ganhador</th><th>Nº Sorte</th>
                <th>Prêmio</th><th>Participantes</th>
              </tr></thead>
              <tbody id=\"ganhadores-recentes-tbody\">
                <tr><td colspan=\"5\" style=\"text-align:center;color:#666;padding:20px\">Carregando...</td></tr>
              </tbody>
            </table>
          </div>
        </div>

        <!-- Histórico de sorteios -->
        <div class=\"table-card\">
          <div class=\"table-header\">
            <h3>📋 Histórico Completo de Sorteios</h3>
          </div>
          <div style=\"overflow-x:auto\">
            <table class=\"data-table\">
              <thead><tr>
                <th>Data</th><th>Ganhador</th><th>Nº Sorte</th>
                <th>Prêmio</th><th>Participantes</th><th>Total Saldo</th>
              </tr></thead>
              <tbody id=\"historico-sorteio-tbody\">
                <tr><td colspan=\"6\" style=\"text-align:center;color:#666;padding:20px\">Carregando...</td></tr>
              </tbody>
            </table>
          </div>
        </div>

      </div><!-- /tab-sorteio -->"""

    if OLD_HIST_END in content:
        content = content.replace(OLD_HIST_END, NEW_HIST_END, 1)
        print("OK: card Ganhadores Recentes adicionado na tab-sorteio")
    else:
        errors.append("ERRO: bloco historico-sorteio final não encontrado")

    # ══════════════════════════════════════════════════════════
    # 2. Atualizar carregarSorteioAdmin para chamar carregarGanhadoresAdmin
    # ══════════════════════════════════════════════════════════
    OLD_CARREGAR_END = "  // Participantes\n  await carregarParticipantes();\n}"

    NEW_CARREGAR_END = """  // Participantes
  await carregarParticipantes();

  // Ganhadores recentes
  await carregarGanhadoresAdmin();
}"""

    if OLD_CARREGAR_END in content:
        content = content.replace(OLD_CARREGAR_END, NEW_CARREGAR_END, 1)
        print("OK: carregarSorteioAdmin agora chama carregarGanhadoresAdmin")
    else:
        errors.append("AVISO: fim carregarSorteioAdmin não encontrado (não crítico)")

    # ══════════════════════════════════════════════════════════
    # 3. Adicionar função carregarGanhadoresAdmin() após limparManuais
    # ══════════════════════════════════════════════════════════
    OLD_LIMPAR_END = "async function salvarConfigSorteio() {"

    NEW_LIMPAR_END = """async function carregarGanhadoresAdmin() {
  const tbody = document.getElementById('ganhadores-recentes-tbody');
  if (!tbody) return;
  try {
    const r = await fetch(`${BASE}/api/sorteio/ganhadores?limit=10`);
    if (!r.ok) { tbody.innerHTML = '<tr><td colspan="5" style="text-align:center;color:#666;padding:20px">Erro ao carregar</td></tr>'; return; }
    const d = await r.json();
    const items = d.ganhadores || [];
    if (!items.length) {
      tbody.innerHTML = '<tr><td colspan="5" style="text-align:center;color:#666;padding:20px">Nenhum ganhador ainda</td></tr>';
      return;
    }
    tbody.innerHTML = items.map((g, i) => {
      const premio = parseFloat(g.premio || 0).toLocaleString('pt-BR', {style:'currency', currency:'BRL'});
      const data = g.criado_em ? new Date(g.criado_em).toLocaleString('pt-BR') : '—';
      const medalha = i === 0 ? '🥇' : i === 1 ? '🥈' : i === 2 ? '🥉' : `${i+1}º`;
      return `<tr>
        <td style="font-size:11px;color:var(--text2)">${data}</td>
        <td><strong>${medalha} ${g.nome || '—'}</strong></td>
        <td style="font-family:monospace;color:#FFD700;font-weight:800">${g.numero || '—'}</td>
        <td style="color:#4ade80;font-weight:700">${premio}</td>
        <td style="text-align:center">${g.participantes || '—'}</td>
      </tr>`;
    }).join('');
  } catch(e) {
    if (tbody) tbody.innerHTML = '<tr><td colspan="5" style="text-align:center;color:#f87171;padding:20px">Erro de conexão</td></tr>';
  }
}

async function salvarConfigSorteio() {"""

    if "async function salvarConfigSorteio() {" in content:
        content = content.replace(OLD_LIMPAR_END, NEW_LIMPAR_END, 1)
        print("OK: função carregarGanhadoresAdmin() adicionada")
    else:
        errors.append("ERRO: salvarConfigSorteio não encontrado")

    # ══════════════════════════════════════════════════════════
    # 4. Adicionar coluna "Data" na tabela de apostas recentes
    # ══════════════════════════════════════════════════════════
    OLD_APOSTAS_HEADER = "                  <th>#</th><th>Usuário</th><th>Jogo</th><th>Seleção</th>\n                  <th>Odd</th><th>Apostado</th><th>Retorno</th><th>Status</th><th>Liga</th><th>Ações</th>"
    NEW_APOSTAS_HEADER = "                  <th>#</th><th>Usuário</th><th>Jogo</th><th>Seleção</th>\n                  <th>Odd</th><th>Apostado</th><th>Retorno</th><th>Status</th><th>Data</th><th>Liga</th><th>Ações</th>"

    if OLD_APOSTAS_HEADER in content:
        content = content.replace(OLD_APOSTAS_HEADER, NEW_APOSTAS_HEADER, 1)
        print("OK: coluna Data adicionada no header da tabela apostas")
    else:
        errors.append("AVISO: header tabela apostas não encontrado (não crítico)")

    # Atualizar colspan do loading (10 -> 11)
    OLD_COLSPAN_APOST = '<tr class="loading-row"><td colspan="10"><div class="spinner"></div></td></tr>'
    NEW_COLSPAN_APOST = '<tr class="loading-row"><td colspan="11"><div class="spinner"></div></td></tr>'
    if OLD_COLSPAN_APOST in content:
        content = content.replace(OLD_COLSPAN_APOST, NEW_COLSPAN_APOST, 1)
        print("OK: colspan loading apostas atualizado 10->11")

    # Atualizar geração da linha na tabela (adicionar coluna data antes de liga)
    OLD_ROW_LIGA = '            <td style="font-size:10px;color:var(--text2);max-width:80px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${nomeLiga}">${nomeLiga}</td>\n            <td>${botoesResolver}</td>\n          </tr>`;\n        }).join(\'\');\n      }\n    }\n  } catch(e) {\n    showToastAdmin(\'Erro ao carregar apostas: \' + e.message, \'red\');'

    NEW_ROW_LIGA = '            <td style="font-size:10px;color:var(--text2)">${a.criado_em ? new Date(a.criado_em).toLocaleDateString(\'pt-BR\') : \'—\'}</td>\n            <td style="font-size:10px;color:var(--text2);max-width:80px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${nomeLiga}">${nomeLiga}</td>\n            <td>${botoesResolver}</td>\n          </tr>`;\n        }).join(\'\');\n      }\n    }\n  } catch(e) {\n    showToastAdmin(\'Erro ao carregar apostas: \' + e.message, \'red\');'

    if '${a.criado_em ? new Date(a.criado_em)' not in content:
        if OLD_ROW_LIGA in content:
            content = content.replace(OLD_ROW_LIGA, NEW_ROW_LIGA, 1)
            print("OK: coluna data adicionada nas linhas da tabela apostas")
        else:
            errors.append("AVISO: linha tabela apostas não encontrada para inserir data (não crítico)")
    else:
        print("OK: coluna data já existe na tabela apostas")

    # Atualizar empty state colspan 10->11
    content = content.replace(
        "colspan=\"10\" style=\"text-align:center;padding:24px;color:var(--text2)\">Nenhuma aposta registrada ainda</td>",
        "colspan=\"11\" style=\"text-align:center;padding:24px;color:var(--text2)\">Nenhuma aposta registrada ainda</td>",
        1
    )

    # Atualizar error state colspan 10->11
    content = content.replace(
        "colspan=\"10\" style=\"text-align:center;color:#f87171;padding:20px\">Erro de conexão",
        "colspan=\"11\" style=\"text-align:center;color:#f87171;padding:20px\">Erro de conexão",
        1
    )

    # ══════════════════════════════════════════════════════════
    # 5. Adicionar endpoints novos na documentação
    # ══════════════════════════════════════════════════════════
    OLD_DOC_SORTEIO = '            <code style="color:#e2e8f0;font-size:14px;">/api/sorteio/set-acumulado</code>'
    NEW_DOC_SORTEIO = '''            <code style="color:#e2e8f0;font-size:14px;">/api/sorteio/set-acumulado</code>
            </div>
            <div style="padding:12px 16px;border-bottom:1px solid var(--border)">
              <span style="background:#22c55e22;color:#4ade80;padding:2px 8px;border-radius:4px;font-size:10px;font-weight:700;margin-right:8px">GET</span>
              <code style="color:#e2e8f0;font-size:14px;">/api/sorteio/meus-bilhetes?cpf=XXX</code>
              <div style="font-size:11px;color:#666;margin-top:4px">Bilhetes do usuário por CPF (público)</div>
            </div>
            <div style="padding:12px 16px;border-bottom:1px solid var(--border)">
              <span style="background:#f59e0b22;color:#fbbf24;padding:2px 8px;border-radius:4px;font-size:10px;font-weight:700;margin-right:8px">POST</span>
              <code style="color:#e2e8f0;font-size:14px;">/api/sorteio/comprar-com-saldo</code>
              <div style="font-size:11px;color:#666;margin-top:4px">Debita saldo da conta e gera bilhetes do sorteio</div>
            </div>
            <div style="padding:12px 16px;border-bottom:1px solid var(--border)">
              <span style="background:#22c55e22;color:#4ade80;padding:2px 8px;border-radius:4px;font-size:10px;font-weight:700;margin-right:8px">GET</span>
              <code style="color:#e2e8f0;font-size:14px;">/api/sorteio/ganhadores?limit=N</code>
              <div style="font-size:11px;color:#666;margin-top:4px">Lista últimos ganhadores do sorteio (público)</div>
            <div style="display:none">'''

    if OLD_DOC_SORTEIO in content and '/api/sorteio/meus-bilhetes' not in content:
        content = content.replace(OLD_DOC_SORTEIO, NEW_DOC_SORTEIO, 1)
        print("OK: endpoints novos adicionados na documentação")
    else:
        print("OK: documentação já contém endpoints novos (sem alteração)")

    if errors:
        print("\n=== AVISOS/ERROS ===")
        for e in errors:
            print(e)

    with open('admin.html', 'w', encoding='utf-8') as f:
        f.write(content)
    print(f"\nadmin.html salvo: {len(content)} chars")

    # Verificações finais
    checks = [
        ('ganhadores-recentes-tbody', 'card Ganhadores Recentes'),
        ('carregarGanhadoresAdmin', 'função carregarGanhadoresAdmin'),
        ('/api/sorteio/ganhadores', 'endpoint sorteio/ganhadores na doc'),
    ]
    for key, label in checks:
        print(f"  {'OK' if key in content else 'FALTA'}: {label}")


if __name__ == '__main__':
    main()
