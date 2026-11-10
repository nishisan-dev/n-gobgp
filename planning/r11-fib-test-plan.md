# R11: plano de testes do encaminhamento Linux orientado por políticas BGP

Registro de 10 de novembro de 2026. Separa o que já foi validado na branch
`codex/linux-policy-routing` dos testes que dependem de equipamento real,
escala, outros kernels ou tempo de execução, e que devem ser feitos antes do
uso em produção.

## Validado nesta entrega

Todos os testes abaixo rodam sem privilégios, em namespaces de usuário, rede
e montagem criados com `unshare`.

| Camada | Como executar | O que cobre |
| --- | --- | --- |
| Configuração | `go test ./pkg/config/oc -run Fib` | Valores padrão, TOML e YAML, rejeição de configurações inválidas (prioridades, protocolos e tabelas reservados, caminhos, âncoras, regras de tráfego, rota default, limites de saúde) |
| Regras | `go test ./pkg/fib -run 'Engine\|Validate'` | Prioridade determinística, communities, AS-PATH, next-hop, prefix-sets, ação ignore, rota default só pela seção própria, condições não suportadas, avisos de GR e Zebra |
| Saúde dos caminhos | `go test ./pkg/fib -run PathHealth` | Limiares de falha e sucesso, queda imediata por link, hold-down de estabilidade, amortecimento de oscilações e retorno ao valor base |
| Gerenciador (kernel simulado) | `go test ./pkg/fib -run 'Anchor\|Routes\|Conflict\|Protected\|Session\|Restart\|Resync\|Traffic\|Default\|Close\|Observe\|Metrics'` | Âncoras, failover, make-before-break, conflitos, prefixos protegidos, perda de sessão, reconciliação por EOR e por prazos, resync, regras e isenções, modos da rota default com restauração, parada limpa, modo observação e métricas |
| Kernel real | `go test ./pkg/fib -run 'Netlink\|MonitorFollows'` | Rotas e regras com protocolo próprio, rotas e regras de terceiros intocadas, substituição e restauração de rota alheia, sonda ARP, detecção de link por eventos netlink |
| Assinatura BGP | `go test ./pkg/server -run SubscribeBestPaths` | Tabela inicial, filtragem por família, retiradas, sessões e End-of-RIB, ausência de descarte com consumidor bloqueado |
| Laboratório ponta a ponta | `test/fib/lab.sh` | Dois `gobgpd` (servidor e CPE em namespace), eBGP multihop entre loopbacks, 48 verificações (abaixo) |

Verificações do laboratório: âncora antes da sessão; rotas em `main`, tabela
100 e tabela 300; prefixo protegido e âncora não sobrescritos pelo BGP;
isenções e regra de tráfego; consulta efetiva da tabela pelo tráfego da LAN;
retirada imediata; falha silenciosa do gateway com o link ativo (âncora, rotas
e default mudam de caminho e a sessão sobrevive além do hold time); hold-down
antes do retorno; queda de link tratada de imediato; os dois caminhos fora
(rotas BGP retiradas, âncora mantida) e recuperação; reparo de rota apagada por
terceiros; `kill -9` seguido de reinício sem lacuna para rotas confirmadas e
para a âncora, com remoção da rota retirada durante a parada; parada limpa sem
objetos próprios e com rotas e regra de terceiros intactas; rota default em
`main` substituindo e restaurando uma default de terceiros na retirada e na
parada.

Limites conhecidos do laboratório: o CPE é outro GoBGP, que só envia
End-of-RIB com graceful restart, então a reconciliação no reinício usa
`eor-timeout` (o caminho por End-of-RIB é coberto pelos testes unitários); o
CPE simula o próprio failover recriando rotas de retorno; não há tráfego real
além de consultas `ip route get`.

## Testes pendentes

Prioridade A deve preceder qualquer uso em produção; B antes de ampliar o
uso; C quando o cenário existir.

| # | Prio. | Teste | Procedimento | Aceite |
| --- | --- | --- | --- | --- |
| 1 | A | Interoperabilidade com o CPE real | Repetir o roteiro do laboratório com o CPE de produção: sessão multihop entre loopbacks, rotas de retorno por enlace, anúncios com communities e AS-PATH reais | Sessão estável durante a falha de cada enlace; decisões iguais às do modo `observe`; nenhuma rota fora da tabela esperada |
| 2 | A | End-of-RIB sem graceful restart | Reiniciar o `gobgpd` com o CPE real e verificar no log o motivo da reconciliação | Reconciliação por End-of-RIB; se o CPE não enviar, ajustar `eor-timeout` ao tempo medido de convergência |
| 3 | A | Implantação em modo `observe` | Rodar dias em `observe` com a configuração final e comparar as decisões com a política esperada | Nenhuma decisão inesperada; nenhum aviso de configuração pendente |
| 4 | A | Acesso de gerência | Com SSH ativo pela rede de gerência: falha de cada enlace, dos dois, `kill -9`, parada limpa e reinício, em cada modo da rota default usado | Sessão SSH nunca interrompida; rotas de gerência intactas |
| 5 | A | Privilégios e systemd | Executar como serviço com apenas `CAP_NET_ADMIN` e `CAP_NET_RAW` (`AmbientCapabilities`) e `Restart=always` | Sondas, rotas e regras funcionam; reinício automático após `kill -9` |
| 6 | A | Gerenciadores de rede | Com NetworkManager, systemd-networkd ou netplan presentes: `netplan apply`, reinício do serviço, renovação de DHCP, `ifdown`/`ifup` dos enlaces | Rotas e regras com o protocolo próprio preservadas, ou reinstaladas pelo resync dentro de `resync-interval`; configurar `ManageForeignRoutes=no` e `ManageForeignRoutingPolicyRules=no` no networkd se necessário |
| 7 | A | `rp_filter` e tráfego assimétrico | Tráfego real (por exemplo `iperf`) pela tabela 100 com cada enlace como caminho ativo, com `rp_filter` estrito e solto | Documentar o valor necessário; sem descarte com o valor recomendado |
| 8 | B | Tipos de interface | Interfaces físicas, VLAN 802.1Q, bond, bridge e PPP (com `probe = "link"`) | Queda de portadora detectada; sonda ARP funcional em interfaces Ethernet; mensagem clara para interfaces sem ARP |
| 9 | B | Falhas além do gateway | CPE com proxy ARP, ou que responde ARP mas não encaminha | Registrar que a falha não é detectada (limite da sonda ARP); avaliar sonda ICMP ou BFD por gateway como evolução |
| 10 | B | Matriz de kernels | Testes `go test ./pkg/fib` e laboratório em Linux 4.18 (RHEL 8), 5.4, 5.15, 6.x | Todos passam; em kernel anterior a 4.17, falha de inicialização clara com regras de tráfego |
| 11 | B | Escala | 100 mil e 1 milhão de destinos em regras de tabela; medir memória, CPU, tempo de carga inicial, tempo de troca de caminho (cada troca percorre todos os destinos) e duração do resync | Números registrados; failover dentro do tempo aceito para o volume real; se não, evoluir para objetos nexthop do kernel |
| 12 | B | Oscilação intensa | Enlace oscilando a cada 100 ms e a cada 5 s por 30 minutos | Trocas limitadas pelo hold-down progressivo; CPU estável; métricas `exit_path_changes_total` coerentes |
| 13 | B | Execução prolongada | 72 horas com falhas periódicas e anúncios variados | Sem crescimento de memória ou goroutines; nenhuma rota órfã após a parada |
| 14 | B | Crash em operações sensíveis | `kill -9` durante a substituição da default (`replace-external`) e durante a troca de tabela | Confirmar e documentar: a default original não é restaurada após crash (limitação conhecida); sem rotas duplicadas após o reinício |
| 15 | B | Mudança de configuração entre execuções | Reiniciar após remover regras, âncoras e regras de tráfego, e após mudar `metric` | Objetos sem dono removidos (âncoras e rotas após a reconciliação, regras na hora); trocar `protocol` deixa as rotas do número antigo, que devem ser removidas manualmente |
| 16 | B | CI no GitHub | Confirmar que o job `r11-fib` roda com namespaces (ajuste de AppArmor no runner) | Job verde e obrigatório no `required` |
| 17 | B | Observabilidade | Coleta Prometheus e alertas para caminho inutilizável, conflito e `reconciled = 0` prolongado | Alertas disparam nos cenários do laboratório |
| 18 | C | Coexistência com FRR | Zebra no mesmo host, em tabela distinta e na mesma tabela | Tabelas distintas sem interferência; na mesma tabela, conflitos registrados e nenhuma rota do FRR alterada |
| 19 | C | Reinício do servidor | Reboot com o serviço habilitado | Início sem rotas herdadas, âncoras antes da sessão, convergência normal |
