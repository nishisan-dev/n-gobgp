# Manual de instalação, configuração e operação do n-gobgp

Este manual cobre o `n-gobgp` como ele existe hoje neste repositório: o
daemon `gobgpd`, o cliente `gobgp`, o arquivo de configuração e a extensão de
encaminhamento Linux (`fib`). Nada aqui foi inventado: cada parâmetro, valor
padrão e comando foi conferido no código-fonte (`pkg/config/oc`,
`pkg/config/kafka`, `pkg/fib`, `cmd/gobgpd`, `cmd/gobgp`) e, quando possível,
executado. Onde uma funcionalidade está apenas planejada, isso é dito de
forma explícita, nunca documentado como se já existisse.

Levantado em 10/11/2026, no commit `c970ee5a` (branch
`codex/linux-policy-routing`), `gobgpd --version`:

```text
gobgpd version n-gobgp 0.1.0-dev commit=c970ee5a2321bcb431b7c6a3ea6a1c2cf95ae2e4 dirty=false upstream=osrg/gobgp@95c43b005b2901279ce055c17d6f2a6fae329336 upstream-version=4.10.0
```

Este manual não substitui os guias específicos já existentes em
`docs/sources/`; ele os referencia onde for o caso, e os reproduz em
português apenas no essencial para as seções pedidas. Para RPKI, BMP, MRT,
BFD, EVPN, Flowspec, Confederação, Route Reflector, Route Server, Dynamic
Neighbor, Peer Group, eBGP Multihop, TTL Security, Sentry e o cliente gRPC,
os guias em inglês (`docs/sources/*.md`) continuam sendo a referência
completa; este manual traz apenas o necessário para a visão de conjunto.

## Sumário

1. [Requisitos e instalação no Linux](#1-requisitos-e-instalação-no-linux)
2. [Estrutura dos arquivos de configuração](#2-estrutura-dos-arquivos-de-configuração)
3. [Parâmetros configuráveis](#3-parâmetros-configuráveis)
4. [Configuração das sessões BGP](#4-configuração-das-sessões-bgp)
5. [Políticas de roteamento, prioridades e fallback](#5-políticas-de-roteamento-prioridades-e-fallback)
6. [Interfaces, next-hops, tabelas Linux e policy routing](#6-interfaces-next-hops-tabelas-linux-e-policy-routing)
7. [Exemplos completos por topologia](#7-exemplos-completos-por-topologia)
8. [Rota default, rotas-âncora e proteção administrativa](#8-rota-default-rotas-âncora-e-proteção-administrativa)
9. [Failover, monitoramento de caminhos e reconciliação](#9-failover-monitoramento-de-caminhos-e-reconciliação)
10. [Diagnóstico, troubleshooting e logs](#10-diagnóstico-troubleshooting-e-logs)
11. [Limitações conhecidas e cuidados operacionais](#11-limitações-conhecidas-e-cuidados-operacionais)

---

## 1. Requisitos e instalação no Linux

### 1.1 Requisitos

- **Go 1.25 ou posterior** para compilar (`go.mod` declara `go 1.25.0`; o CI
  do fork compila e testa com `1.27.3`). Não há binários pré-compilados
  publicados por este repositório; a instalação é sempre por compilação a
  partir do código-fonte.
- **Linux** para a extensão `fib` (`pkg/fib/platform_other.go` recusa o modo
  `enforce` em qualquer outro sistema operacional, retornando erro). O
  restante do `gobgpd` (sessões BGP, políticas, RPKI, BMP, MRT, Zebra,
  Kafka) compila para FreeBSD, OpenBSD, macOS e Windows além de Linux — o CI
  do fork verifica apenas a compilação cruzada (`go build`) para esses
  quatro sistemas, não a execução; a suíte de testes roda apenas em Linux.
- **Kernel Linux 4.17 ou posterior** apenas se forem usadas regras de
  tráfego (`fib.traffic-rules`), porque é a versão que introduziu o atributo
  de protocolo em `ip rule` usado para identificar a posse das regras
  (`pkg/fib/kernel_linux.go`, função `kernelRequirements`). Rotas por si só
  não têm esse requisito.
- **Privilégios de kernel** para o modo `enforce`: `CAP_NET_ADMIN` (rotas e
  regras via netlink) e `CAP_NET_RAW` (sondas ARP, `pkg/fib/probe_linux.go`
  abre um socket `AF_PACKET`). Sem essas capacidades, `gobgpd` falha ao
  iniciar com `fib.mode = "enforce"`.
- Nenhuma dependência de pacotes do sistema é necessária para compilar; o
  `go.mod` resolve todas as bibliotecas Go. O cliente Zebra integra com FRR
  ou Quagga já instalados separadamente, se essa funcionalidade for usada
  (seção 11 e [`docs/sources/zebra.md`](zebra.md)).

### 1.2 Compilação

```sh
git clone https://github.com/nishisan-dev/n-gobgp.git
cd n-gobgp
mkdir -p bin
go build -o bin/gobgpd ./cmd/gobgpd
go build -o bin/gobgp ./cmd/gobgp
```

O módulo Go mantém o nome original `github.com/osrg/gobgp/v4`. Em AMD64, o
`CONTRIBUTING.md` recomenda `GOAMD64=v3` antes do `go build` para melhor
desempenho em processadores recentes; isso é opcional.

Confirme a versão e a origem do binário:

```sh
bin/gobgpd --version
```

### 1.3 Validação da configuração antes de instalar

`gobgpd` tem um modo de verificação que lê e valida o arquivo de
configuração (incluindo a seção `fib`, ver seção 3.8) sem iniciar nenhuma
sessão:

```sh
bin/gobgpd -f /etc/gobgpd/gobgpd.conf -d
```

A opção é `-d`/`--dry-run`. Em caso de erro, o processo termina com código de
saída 1 e uma mensagem; em caso de sucesso, apenas confirma a leitura do
arquivo (e, com `--log-level debug`, imprime a configuração carregada).

### 1.4 Execução manual

```sh
sudo bin/gobgpd -f /etc/gobgpd/gobgpd.conf
```

A API gRPC de gerência escuta por padrão em `:50051`, em todas as
interfaces, sem autenticação (`--api-hosts`, `--tls`, `--tls-cert-file` etc.
controlam isso; ver seção 10.1). Restringir o acesso é responsabilidade de
quem opera o serviço.

### 1.5 Executando como serviço systemd

O repositório traz um exemplo de unidade em
`tools/contrib/centos/gobgpd.service`:

```ini
[Unit]
Description=GoBGP Routing Daemon
Wants=network.target
After=network.target

[Service]
Type=notify
ExecStartPre=/usr/bin/gobgpd -f /etc/gobgpd/gobgpd.conf -d
ExecStart=/usr/bin/gobgpd -f /etc/gobgpd/gobgpd.conf --sdnotify
ExecReload=/usr/bin/kill -HUP $MAINPID
StandardOutput=journal
StandardError=journal
User=gobgpd
Group=gobgpd
AmbientCapabilities=CAP_NET_BIND_SERVICE

[Install]
WantedBy=multi-user.target
```

- `Type=notify` funciona porque `gobgpd --sdnotify` usa o protocolo
  `sd_notify` (`READY=1` ao terminar a inicialização, `STOPPING=1` ao parar;
  `cmd/gobgpd/main.go`, `notifyReady`/`stopServer`).
  `ExecStartPre` com `-d` falha o serviço antes de trocar o processo
  principal se a configuração estiver incorreta.
- `AmbientCapabilities=CAP_NET_BIND_SERVICE` é necessária para escutar na
  porta BGP padrão (179) sem rodar como root.
- **Se `fib.mode = "enforce"` estiver em uso**, acrescente
  `CAP_NET_ADMIN` e `CAP_NET_RAW` à lista de capacidades:

  ```ini
  AmbientCapabilities=CAP_NET_BIND_SERVICE CAP_NET_ADMIN CAP_NET_RAW
  ```

  O item 5 do plano de testes em `planning/r11-fib-test-plan.md` cobre a
  validação dessa execução com privilégios mínimos.
- **Recomendado para o `fib`:** `Restart=always` (ou `on-failure`) na seção
  `[Service]`. Um `kill -9` deixa as rotas instaladas, sem failover, até o
  processo voltar (seção 9.5); um reinício automático minimiza essa janela.
- A opção `-a`/`--config-auto-reload` do `gobgpd` observa o arquivo de
  configuração e dispara o equivalente a um `SIGHUP` quando ele muda,
  limitado a um evento a cada 100 ms. Ela não substitui `ExecReload`, que
  envia o sinal diretamente.

---

## 2. Estrutura dos arquivos de configuração

### 2.1 Formatos aceitos

`gobgpd` lê o arquivo indicado por `-f`/`--config-file` no formato indicado
por `-t`/`--config-type` (`toml`, `yaml` ou `json`; `toml` é o padrão). Se a
extensão do arquivo for `.toml`, `.yaml`/`.yml` ou `.json`, ela sobrepõe o
valor de `-t` (`pkg/config/oc/util.go`, `detectConfigFileType`). Nenhum outro
formato é reconhecido.

Todas as chaves são indiferentes a maiúsculas/minúsculas, porque o
carregamento é feito por [Viper](https://github.com/spf13/viper); os exemplos
deste manual usam o nome em kebab-case usado nos demais guias do projeto.

### 2.2 Seções de nível superior

O arquivo de configuração é um único objeto com estas seções, todas
opcionais exceto `global`:

| Seção | Conteúdo | Aplicada quando |
| --- | --- | --- |
| `[global]` | AS local, router-id, porta, endereços de escuta, seleção de rota, política global, graceful restart padrão, confederação | Só na inicialização |
| `[[neighbors]]` | Cada vizinho BGP | Inicialização e recarga (`SIGHUP`) |
| `[[peer-groups]]` | Modelos de configuração reutilizados por vizinhos | Inicialização e recarga |
| `[[dynamic-neighbors]]` | Prefixos que aceitam conexões de entrada vinculadas a um peer-group | Inicialização e recarga |
| `[[keychains]]` | Chaves TCP-AO | Inicialização e recarga |
| `[defined-sets]` e `[[policy-definitions]]` | Condições e políticas de roteamento BGP | Inicialização e recarga |
| `[[rpki-servers]]` | Servidores RPKI (RTR) | Só na inicialização |
| `[[bmp-servers]]` | Estações BMP | Só na inicialização |
| `[[vrfs]]` | VRFs (MPLS L3VPN) | Só na inicialização |
| `[[mrt-dump]]` | Despejo MRT | Só na inicialização |
| `[zebra]` | Integração com FRR/Quagga | Só na inicialização |
| `[kafka]` | Publicação de eventos BGP em tópicos Kafka (extensão do fork) | Só na inicialização |
| `[fib]` | Encaminhamento Linux orientado por políticas BGP (extensão do fork) | Só na inicialização |

"Só na inicialização" significa que a seção é processada por
`config.InitialConfig` e **não** é reprocessada por `config.UpdateConfig`
quando o arquivo é recarregado (`SIGHUP` ou `--config-auto-reload`). Mudar
essas seções exige reiniciar o processo. Isso foi confirmado lendo
`pkg/config/config.go`: `UpdateConfig` recalcula apenas políticas,
peer-groups, vizinhos, vizinhos dinâmicos e chaveiros TCP-AO; `Zebra`,
`RpkiServers`, `BmpServers`, `Vrfs`, `MrtDump` e `Global` não aparecem ali.
`Kafka` e `Fib` são explicitamente comparados e, se mudarem, `gobgpd` registra
um aviso e mantém a configuração ativa (`cmd/gobgpd/main.go`).

### 2.3 Exemplo mínimo

```toml
[global.config]
  as = 65001
  router-id = "192.0.2.1"

[[neighbors]]
  [neighbors.config]
    neighbor-address = "192.0.2.2"
    peer-as = 65002
```

Um exemplo mais completo, cobrindo a maioria das seções do protocolo BGP,
está em [`docs/sources/configuration.md`](configuration.md). Um exemplo
completo da seção `fib` está em [`docs/examples/fib.toml`](../examples/fib.toml)
e na seção 7.3 deste manual.

### 2.4 Recarga de configuração

- `kill -HUP <pid>` ou `--config-auto-reload` disparam uma releitura do
  arquivo e aplicam as diferenças nas seções que suportam recarga (vizinhos,
  peer-groups, vizinhos dinâmicos, chaveiros TCP-AO e políticas). Erros de
  aplicação são registrados em log, sem derrubar o processo
  (`cmd/gobgpd/main.go`, comentário: "it shouldn't flush policy entirely, so
  it's safe to continue to run").
- Mudar a política de importação/exportação dispara automaticamente um soft
  reset de entrada (`ResetPeerRequest` com `Soft: true`) para reavaliar as
  rotas já recebidas.
- `Kafka` e `Fib` nunca são reaplicados numa recarga; a configuração ativa é
  preservada e um aviso é escrito no log.

---

## 3. Parâmetros configuráveis

Esta seção lista os parâmetros com valor padrão e restrição, conforme
`pkg/config/oc/bgp_configs.go`, `pkg/config/oc/default.go`,
`pkg/config/kafka/config.go` e `pkg/config/oc/fib.go`. As seções de
protocolo com modelo OpenConfig/YANG completo (RPKI, BMP, MRT, BFD, EVPN,
Flowspec, SR Policy, MUP, confederação) têm guias próprios já corretos; aqui
trazemos a referência mínima e apontamos para eles.

### 3.1 `[global.config]`

| Parâmetro | Tipo | Padrão | Restrição |
| --- | --- | --- | --- |
| `as` | inteiro | — | Número de sistema autônomo local |
| `router-id` | endereço IPv4 | — | Identificador do roteador |
| `port` | inteiro | `179` | `-1` desativa a escuta TCP de entrada |
| `local-address-list` | lista de endereços | `["0.0.0.0", "::"]` | Endereços em que o `gobgpd` aceita conexões de entrada |
| `bind-to-device` | string | — | Nome de interface para vincular o socket de escuta |

Dentro de `[global]` também existem `apply-policy` (seção 5), `confederation`,
`graceful-restart` (ver [graceful-restart.md](graceful-restart.md)),
`use-multiple-paths` (ECMP, ver 3.2), `route-selection-options` e
`default-route-distance`.

### 3.2 Seleção de rota e ECMP (`[global]`)

| Parâmetro | Padrão | Restrição |
| --- | --- | --- |
| `route-selection-options.config.always-compare-med` | `false` | Compara MED mesmo entre AS de origem diferentes |
| `route-selection-options.config.ignore-as-path-length` | `false` | Ignora o comprimento do AS-PATH na seleção |
| `route-selection-options.config.external-compare-router-id` | `false` | Usa o router-id para desempate entre rotas eBGP |
| `use-multiple-paths.config.enabled` | `false` | Habilita múltiplos caminhos na RIB (ECMP) |
| `use-multiple-paths.ebgp.config.allow-multiple-as` | `false` | Permite ECMP entre caminhos eBGP de AS de origem diferentes |
| `use-multiple-paths.ebgp.config.maximum-paths` | sem limite | Teto de caminhos eBGP instalados |
| `use-multiple-paths.ibgp.config.maximum-paths` | sem limite | Teto de caminhos iBGP instalados |

A extensão `fib` (seção 6) consome o melhor caminho da RIB global; com ECMP
habilitado, o comportamento de instalação no kernel ainda segue uma lista
ordenada de caminhos de saída definida pela regra `fib`, não os múltiplos
caminhos do ECMP do BGP (ver seção 11).

### 3.3 `[[neighbors]]` e `[[peer-groups]]`

Os parâmetros comuns de um vizinho estão detalhados na seção 4. Um vizinho
herda de um `peer-group` pelos campos não preenchidos; campos definidos
diretamente no vizinho prevalecem (`docs/sources/peer-group.md`).

### 3.4 `[[dynamic-neighbors]]`

| Parâmetro | Restrição |
| --- | --- |
| `config.prefix` | Prefixo IPv4/IPv6 de onde conexões de entrada são aceitas |
| `config.peer-group` | Nome de um `peer-group` já definido; obrigatório |

Mais detalhes em [`docs/sources/dynamic-neighbor.md`](dynamic-neighbor.md).

### 3.5 `[[rpki-servers]]`

| Parâmetro | Padrão | Observação |
| --- | --- | --- |
| `config.address`, `config.port` | — | Endereço/porta do cache RTR |
| `config.refresh-time` | — | Intervalo de verificação |
| `config.hold-time` | — | Tempo sem atividade antes de considerar a sessão inoperante |
| `config.record-lifetime` | — | Validade dos registros recebidos |
| `config.preference` | — | Prioridade estática entre caches |

Detalhes de uso em [`docs/sources/rpki.md`](rpki.md), incluindo a
revalidação automática de políticas ao receber novo lote de VRPs,
implementada neste fork.

### 3.6 `[[bmp-servers]]`

| Parâmetro | Padrão | Restrição |
| --- | --- | --- |
| `config.address`, `config.port` | — | Estação BMP |
| `config.route-monitoring-policy` | `pre-policy` | `pre-policy`, `post-policy`, `both` (obsoleto), `local-rib`, `all` |
| `config.statistics-timeout` | — | Se diferente de zero, deve ser `>= 15` |
| `config.route-mirroring-enabled` | `false` | Espelha mensagens BGP recebidas |
| `config.sys-name`, `config.sys-descr` | — | Identificação enviada à estação |

Detalhes em [`docs/sources/bmp.md`](bmp.md).

### 3.7 `[[vrfs]]`, `[[mrt-dump]]`, `[zebra]`

| Seção | Parâmetros principais | Padrão/Restrição |
| --- | --- | --- |
| `vrfs.config` | `name`, `id` (gerado se omitido), `rd`, `import-rt-list`/`export-rt-list`/`both-rt-list` | `import-rt-list`/`export-rt-list` têm prioridade sobre `both-rt-list` se definidos |
| `mrt-dump.config` | `dump-type` (`updates` ou `table`), `file-name`, `table-name`, `dump-interval`, `rotation-interval` | Formato do nome do arquivo segue o pacote `time` do Go |
| `zebra.config` | `enabled`, `url`, `redistribute-route-type-list`, `version`, `nexthop-trigger-enable`, `nexthop-trigger-delay` (padrão `5`), `mpls-label-range-size`, `software-name` | `url` padrão `unix:/var/run/quagga/zserv.api` quando omitido |

Detalhes em [`docs/sources/zebra.md`](zebra.md),
[`docs/sources/zebra-multipath.md`](zebra-multipath.md) e
[`docs/sources/mrt.md`](mrt.md).

### 3.8 `[kafka]` (extensão do fork)

Seção opcional para publicar eventos BGP (atualizações, retiradas, estado de
peers, instantâneo inicial) em um ou mais clusters Kafka, com filtros por
sink, fila limitada e spool durável. Totalmente documentada em
[`docs/sources/kafka-event-sinks.md`](kafka-event-sinks.md) e
[`docs/examples/kafka-event-sinks.yaml`](../examples/kafka-event-sinks.yaml);
a tabela de parâmetros completa, com padrões e validações, está lá e não é
repetida aqui para não divergir. Pontos relevantes para este manual:

- Desligado por padrão (`kafka.enabled = false`); sem efeito sobre o
  comportamento do BGP quando desligado.
- A configuração é validada por `config.Kafka.Normalize()` na leitura do
  arquivo, incluindo em `--dry-run`.
- Mudanças em `[kafka]` exigem reinício (seção 2.4).

### 3.9 `[fib]` (extensão do fork)

Encaminhamento Linux orientado por políticas BGP. É o assunto central das
seções 5 a 9 deste manual. A tabela de parâmetros completa está na seção
5.3, 6 e 8. Pontos gerais:

| Parâmetro | Padrão | Restrição |
| --- | --- | --- |
| `enabled` | `false` | Sem esta seção ou com `enabled = false`, nada muda no comportamento atual |
| `mode` | `observe` | `observe` (só registra a decisão em log) ou `enforce` (programa o kernel). Qualquer outro valor é rejeitado |
| `protocol` | `200` | Número de protocolo de rota/regra usado para marcar a posse. Rejeita números usados pelo kernel, pelo iproute2 e por outros daemons de roteamento conhecidos (incluindo a faixa do FRR): `0,1,2,3,4,8–18,42,99,186–198` |
| `metric` | `20` | Métrica de toda rota instalada |
| `anchor-wait` | `10s` | Tempo de espera pelas âncoras antes de iniciar os vizinhos BGP |
| `resync-interval` | `30s` | Período da reconciliação periódica com o kernel; mínimo `1s` |
| `protected-prefixes` | nenhum | Lista de prefixos IPv4 mascarados que rotas de BGP, iguais ou mais específicas, não podem ocupar na tabela `main` |
| `rule-priority-min` / `rule-priority-max` | `1000` / `1999` | Faixa de prioridade das `ip rule` gerenciadas; deve formar um intervalo abaixo de `32766` (a regra `main` do próprio kernel) |

A seção é validada por `config.Fib.Normalize()` na leitura do arquivo,
incluindo em `--dry-run`, e novamente de forma cruzada com o resto da
configuração por `fib.Validate` (avisos sobre Graceful Restart e Zebra,
seção 5.4).

---

## 4. Configuração das sessões BGP

Esta seção resume os parâmetros de uma sessão BGP individual
(`[[neighbors]]` ou `[[peer-groups]]`). Os exemplos completos de cada
funcionalidade têm guia próprio (coluna "Guia"); os parâmetros vêm de
`pkg/config/oc/bgp_configs.go` e os padrões de `pkg/config/oc/default.go`.

### 4.1 Identificação e AS

| Parâmetro (`neighbors.config`) | Padrão | Observação |
| --- | --- | --- |
| `neighbor-address` | — | Obrigatório, exceto com `neighbor-interface` (BGP unnumbered) |
| `neighbor-interface` | — | Alternativa a `neighbor-address`; endereço link-local IPv6 resolvido pela interface. Não suportado com `route-server-client = true` |
| `peer-as` | — | AS remoto esperado; `0` desativa a checagem |
| `local-as` | AS de `global.config.as` | Sobrepõe o AS local anunciado a este vizinho |
| `peer-type` | calculado por `peer-as` | `internal` ou `external`, pode ser forçado |
| `peer-group` | — | Nome de um `[[peer-groups]]` já definido |
| `admin-down` | `false` | Mantém o vizinho administrativamente desligado |
| `description` | — | Texto livre |
| `vrf` | — | Associa o vizinho a uma VRF (ver 3.7) |
| `send-software-version` | `false` | Envia a capacidade de versão de software |
| `auth-password` | — | Senha MD5 da sessão TCP (RFC 2385) |
| `remove-private-as` | — | `all` ou `replace` |
| `route-flap-damping` | `false` | — |
| `send-community` | — | `standard`, `extended`, `both` ou `none` |

### 4.2 Temporizadores (`neighbors.timers.config`)

| Parâmetro | Padrão |
| --- | --- |
| `connect-retry` | `120` s |
| `hold-time` | `90` s |
| `keepalive-interval` | `hold-time / 3` (calculado se omitido) |
| `idle-hold-time-after-reset` | `30` s |
| `minimum-advertisement-interval` | — |

### 4.3 Transporte (`neighbors.transport.config`)

| Parâmetro | Observação |
| --- | --- |
| `passive-mode` | Não inicia conexões de saída; só aceita |
| `local-address` | Endereço de origem da sessão |
| `local-port`, `remote-port` | Porta TCP não padrão |
| `tcp-mss`, `mtu-discovery` | Ajustes de TCP |
| `ip-tos` | Byte DSCP/ToS do tráfego da sessão |
| `bind-interface` | Interface a que o socket é vinculado |

### 4.4 Sessão indireta e proteção

| Funcionalidade | Seção | Observação | Guia |
| --- | --- | --- | --- |
| eBGP Multihop | `neighbors.ebgp-multihop.config` | `enabled`, `multihop-ttl` (padrão `255` quando habilitado) | [ebgp-multihop.md](ebgp-multihop.md) |
| TTL Security (GTSM) | `neighbors.ttl-security.config` | `enabled`, `ttl-min` (padrão `255` quando habilitado) | [ttl-security.md](ttl-security.md) |
| BFD | `neighbors.bfd.config` | `enabled`, `desired-minimum-tx-interval`, `required-minimum-receive`, `detection-multiplier` (padrão `3`), `port` (padrão `3784`) | [bfd.md](bfd.md) |
| TCP-AO | `neighbors.tcp-ao.config` (`keychain`, `send-id`) + `[[keychains]]` + CLI `gobgp keychain add/del/update` | Chaveiros são geridos principalmente pela API/CLI | — |

**eBGP Multihop e TTL Security são mutuamente exclusivos no mesmo
vizinho.** Isso é relevante para a topologia de sessão única entre loopbacks
descrita na seção 7.3: se for necessário blindar contra spoofing na sessão
multihop, use `ttl-min` coerente com o número de saltos reais, nunca os dois
mecanismos juntos.

### 4.5 Graceful Restart e Long-Lived Graceful Restart

Seção `neighbors.graceful-restart.config`: `enabled`, `restart-time`
(padrão igual ao `hold-time` se omitido), `notification-enabled`,
`long-lived-enabled`, `deferral-time` (padrão `360`). Por família, em
`neighbors.afi-safis[].mp-graceful-restart.config` e
`.long-lived-graceful-restart.config`. Guia completo em
[graceful-restart.md](graceful-restart.md).

**Atenção para quem usa a extensão `fib` (seção 9):** a reconciliação de
rotas no reinício do `gobgpd` espera o fim da RIB (End-of-RIB) de cada
sessão estabelecida, e trata caminhos marcados como obsoletos (stale) por
Graceful Restart como ausentes. `fib.Validate` emite um aviso quando um
vizinho usado pela extensão tem Graceful Restart ou Long-Lived Graceful
Restart habilitado.

### 4.6 Limite de prefixos (`neighbors.afi-safis[].prefix-limit.config`)

| Parâmetro | Observação |
| --- | --- |
| `max-prefixes` | Limite de prefixos aceitos da família |
| `shutdown-threshold-pct` | Percentual de `max-prefixes` que gera aviso antes do limite |
| `restart-timer` | Tempo até a sessão ser restabelecida após ultrapassar o limite |

### 4.7 ADD-PATH, Route Reflector, Route Server

| Funcionalidade | Seção | Guia |
| --- | --- | --- |
| ADD-PATH | `neighbors.add-paths.config` (`receive`, `send-max`), por família em `neighbors.afi-safis[].add-paths.config` | [add-paths.md](add-paths.md) |
| Route Reflector | `neighbors.route-reflector.config` (`route-reflector-client`, `route-reflector-cluster-id`) | [route-reflector.md](route-reflector.md) |
| Route Server | `neighbors.route-server.config` (`route-server-client`, `secondary-route`) | [route-server.md](route-server.md) |
| Confederação | `global.confederation` | [bgp-confederation.md](bgp-confederation.md) |

### 4.8 Famílias de endereço (`[[neighbors.afi-safis]]`)

`afi-safi-name` aceita exatamente estes valores (`pkg/config/oc/bgp_configs.go`):

```text
ipv4-unicast        ipv6-unicast
ipv4-multicast      ipv6-multicast
ipv4-labelled-unicast        ipv6-labelled-unicast
l3vpn-ipv4-unicast  l3vpn-ipv6-unicast
l3vpn-ipv4-multicast          l3vpn-ipv6-multicast
l2vpn-vpls          l2vpn-evpn
rtc
ipv4-encap          ipv6-encap
ipv4-flowspec       ipv6-flowspec
l3vpn-ipv4-flowspec l3vpn-ipv6-flowspec
l2vpn-flowspec
ipv4-srpolicy       ipv6-srpolicy
opaque
ls
ipv4-mup            ipv6-mup
```

Se `[[neighbors.afi-safis]]` for omitido, o padrão depende da forma como o
vizinho é identificado: `ipv4-unicast` e `ipv6-unicast` juntos quando o
vizinho usa `neighbor-interface` (BGP unnumbered, seção 4.1); caso
contrário, apenas a família correspondente ao endereço de
`neighbor-address` (IPv4 ou IPv6).

### 4.9 Diagnóstico de uma sessão específica

```sh
gobgp neighbor
gobgp neighbor <endereço>
gobgp neighbor <endereço> adj-in
gobgp neighbor <endereço> adj-out
gobgp neighbor <endereço> local
```

Detalhes e saídas de exemplo em
[`docs/sources/getting-started.md`](getting-started.md) e
[`docs/sources/cli-operations.md`](cli-operations.md).

---

## 5. Políticas de roteamento, prioridades e fallback

Há **duas camadas de política distintas** neste projeto, e elas não devem
ser confundidas:

1. **Política BGP** (`[defined-sets]` + `[[policy-definitions]]`): decide se
   uma rota é aceita/rejeitada e permite alterar seus atributos (community,
   MED, local-pref, next-hop, AS-PATH) na entrada ou na saída de cada
   vizinho ou da RIB global. Não decide nada sobre o kernel Linux.
2. **Regras `fib`** (`[[fib.rules]]`): decidem, para o melhor caminho já
   aceito pela política BGP, se ele é instalado no Linux, em qual tabela, e
   por qual lista ordenada de caminhos de saída. Não alteram atributos BGP.

Use a política BGP para decidir **o que entra na RIB**; use as regras `fib`
para decidir **como isso é encaminhado no Linux**.

### 5.1 Política BGP: modelo e prioridade

Uma política (`policy-definitions`) é uma lista de `statements`, avaliados
em ordem. Cada `statement` tem condições (`conditions`) e ações
(`actions`). Quando **todas** as condições de um `statement` são
verdadeiras, suas ações são aplicadas; se o `statement` definir
`route-disposition`, a avaliação da política para naquele ponto
(`accept-route` ou `reject-route`). Um `statement` sem `route-disposition`
aplica suas ações de modificação e segue para o próximo `statement` (o
"fallback" natural desse modelo é a avaliação sequencial e o
`default-import-policy`/`default-export-policy`, usado quando nenhuma
política ou nenhum `statement` decide o destino da rota).

Condições disponíveis: prefixo, vizinho, AS-PATH, comprimento do AS-PATH,
community, community estendida, community grande, resultado de validação
RPKI, tipo de rota (local/interno/externo), `afi-safi-in`, local-pref, MED.
Ações disponíveis: aceitar/rejeitar, somar/substituir/remover community (ou
remover todas), somar/subtrair/substituir MED, definir next-hop
(endereço específico, `self`, `peer-address` ou `unchanged`), definir
local-pref, repetir AS no AS-PATH (`0` é sempre rejeitado, RFC 7607).

A referência completa, com todas as tabelas de parâmetros e dezenas de
exemplos, está em [`docs/sources/policy.md`](policy.md); ela não é
reproduzida aqui para não divergir. Pontos que valem reforçar para quem vem
da seção de `fib`:

- A política é anexada à RIB global em `global.apply-policy.config`, ou por
  vizinho/peer-group em `neighbors.apply-policy.config`
  (`import-policy-list`, `export-policy-list`, `default-import-policy`,
  `default-export-policy`).
- Mudar a política dispara automaticamente um soft reset de entrada (seção
  2.4), sem precisar reiniciar o processo.
- `gobgp policy`, `gobgp policy statement`, `gobgp policy prefix`,
  `gobgp policy community`, `gobgp policy as-path`, `gobgp policy neighbor`,
  `gobgp global policy import|export` e
  `gobgp neighbor <endereço> policy import|export` inspecionam o estado
  atual (seção 10).

### 5.2 Exemplo de política BGP

```toml
[[defined-sets.prefix-sets]]
  prefix-set-name = "customers"
  [[defined-sets.prefix-sets.prefix-list]]
    ip-prefix = "198.51.100.0/24"
    masklength-range = "24..32"

[[defined-sets.bgp-defined-sets.community-sets]]
  community-set-name = "no-export-to-upstream"
  community-list = ["65001:999"]

[[policy-definitions]]
  name = "import-from-cpe"
  [[policy-definitions.statements]]
    name = "reject-tagged"
    [policy-definitions.statements.conditions.bgp-conditions.match-community-set]
      community-set = "no-export-to-upstream"
    [policy-definitions.statements.actions]
      route-disposition = "reject-route"
  [[policy-definitions.statements]]
    name = "accept-customers"
    [policy-definitions.statements.conditions.match-prefix-set]
      prefix-set = "customers"
    [policy-definitions.statements.actions]
      route-disposition = "accept-route"

[global.apply-policy.config]
  import-policy-list = ["import-from-cpe"]
  default-import-policy = "accept-route"
```

### 5.3 Regras `fib`: modelo, prioridade e fallback

Cada regra (`[[fib.rules]]`) tem:

| Campo | Padrão | Restrição |
| --- | --- | --- |
| `name` | — | Obrigatório, único, até 64 caracteres (`[A-Za-z0-9._-]`) |
| `priority` | — | Obrigatório, maior que zero, **único entre todas as regras** |
| `family` | `ipv4-unicast` | É o único valor aceito nesta versão (ver seção 11) |
| `action` | `install` | `install` ou `ignore` |
| `table` | — | Obrigatório para `action = "install"`: `"main"` ou um número de tabela que não seja reservado pelo kernel (`0`, `253`, `255`) |
| `exit-paths` | — | Obrigatório para `action = "install"`: lista ordenada de nomes definidos em `fib.exit-paths`, sem repetição |
| `conditions` | — | Mesma sintaxe de `policy-definitions.statements.conditions`, resolvida contra os mesmos `[defined-sets]` do arquivo |

A engine (`pkg/fib/rules.go`, `Engine.Evaluate`) ordena as regras pela
`priority` crescente e aplica **a primeira cuja condição seja satisfeita**
pelo melhor caminho da RIB global. Esse é o mecanismo de prioridade e de
fallback desta camada: uma regra com `priority` menor é tentada primeiro; se
não casar, a próxima (maior `priority`) é tentada, e assim por diante. Uma
regra "pega-tudo" sem `conditions` sempre casa, e normalmente deve ter a
maior `priority` do conjunto, funcionando como fallback final. Um destino
que não casa com **nenhuma** regra não é instalado (nem na tabela `main`).

Dentro de uma regra com `action = "install"`, `exit-paths` é também uma
lista de fallback, mas em outro nível: entre os caminhos de saída de
**uma mesma regra**, o primeiro que estiver saudável (seção 9) é usado; os
demais só entram em uso se os anteriores falharem. Isso é o mecanismo de
failover entre interfaces, não de seleção entre regras.

Condições sem equivalente fora do ponto de atribuição de política são
rejeitadas na carga da configuração: `call-policy`, `match-tag-set`,
`install-protocol-eq`, `rpki-validation-result` e `afi-safi-in-list`
(`pkg/fib/rules.go`, `unsupportedConditions`).

**A regra genérica nunca casa com a rota default** (`0.0.0.0/0`): isso é
tratado por uma seção própria, `fib.default-route` (seção 8).

### 5.4 Avisos de configuração (`fib.Validate`)

Ao ligar `fib.enabled = true`, além da validação estrutural, o carregamento
roda `fib.Validate` e registra avisos (não erros) quando:

- um vizinho tem Graceful Restart ou Long-Lived Graceful Restart habilitado
  (seção 4.5);
- o Zebra está habilitado e alguma regra `fib` usa a tabela `main`
  (coexistência, seção 11).

---

## 6. Interfaces, next-hops, tabelas Linux e policy routing

### 6.1 Caminhos de saída (`[[fib.exit-paths]]`)

Um caminho de saída é a unidade que a extensão `fib` usa para encaminhar:
uma interface Linux e o gateway alcançado por ela, independente do next-hop
anunciado pelo BGP.

| Campo | Padrão | Restrição |
| --- | --- | --- |
| `name` | — | Obrigatório, único |
| `interface` | — | Obrigatório; nome de interface Linux válido (até 15 caracteres, sem `/` nem espaço) |
| `gateway` | — | Obrigatório; endereço IPv4 unicast (não `0.0.0.0`, não multicast) |
| `probe` | `arp` | `arp` (o gateway precisa responder ARP) ou `link` (só o estado do link conta, para interfaces sem ARP) |

Uma regra `fib.rules[].exit-paths` referencia esses nomes, em ordem de
preferência. **O next-hop efetivo instalado no kernel é o `gateway` do
primeiro caminho de saída saudável da regra, não o next-hop anunciado pelo
BGP.** Isso atende diretamente ao requisito de poder usar um gateway
diferente do anunciado.

### 6.2 Tabelas Linux e policy routing

- `table = "main"` instala a rota na tabela principal do Linux (`254`).
- Qualquer outro número de tabela (exceto os reservados `0`, `253`, `255`)
  pode ser usado como tabela alternativa de policy routing.
- Para que o tráfego de fato consulte essa tabela alternativa, é preciso uma
  regra de tráfego (`[[fib.traffic-rules]]`, equivalente a `ip rule`), que é
  uma camada **independente** das regras de seleção de rota: as regras
  `fib.rules` decidem o que vai para cada tabela; as `fib.traffic-rules`
  decidem qual tráfego consulta cada tabela. Nada liga automaticamente
  tráfego a uma tabela além da `main`.

| Campo (`fib.traffic-rules[]`) | Restrição |
| --- | --- |
| `name` | Obrigatório, único |
| `priority` | Obrigatório, único entre as regras de tráfego, dentro da faixa `[rule-priority-min + N, rule-priority-max]`, onde `N` é o número de prefixos protegidos mais o número de âncoras (essas geram regras de isenção automáticas antes das suas) |
| `table` | Obrigatório: `"main"` ou um número de tabela |
| `from`, `to` | Opcionais; prefixos IPv4 mascarados |
| `iif` | Opcional; interface de entrada, mesma restrição de nome que `interface` |
| `fwmark` | Opcional; `"marca"` ou `"marca/máscara"`, decimal ou hexadecimal (`0x...`) |

Exemplo: enviar o tráfego de uma LAN interna pela tabela 100, mantendo tudo
o mais na `main`:

```toml
[[fib.traffic-rules]]
  name = "lan-para-wan2"
  priority = 1010
  table = 100
  from = "172.16.0.0/16"
```

### 6.3 Posse e não interferência

Toda rota e toda `ip rule` instalada pelo `fib` carrega o número de
protocolo configurado (`fib.protocol`, padrão `200`). Rotas e regras com
outro número de protocolo nunca são alteradas, substituídas ou removidas,
exceto o caso explícito e opt-in da rota default (seção 8.3). Um conflito
(mesma tabela, prefixo e métrica já ocupados por outro dono) é registrado em
log e métrica, e tentado de novo pela reconciliação periódica (seção 9.4),
sem jamais sobrescrever o objeto alheio.

---

## 7. Exemplos completos por topologia

### 7.1 Dois peers eBGP simples, sem `fib`

Equivalente ao [Getting Started](getting-started.md), sem nenhuma extensão
deste fork — útil como ponto de partida mínimo.

```toml
[global.config]
  as = 64512
  router-id = "192.168.255.1"

[[neighbors]]
  [neighbors.config]
    neighbor-address = "10.0.255.1"
    peer-as = 65001

[[neighbors]]
  [neighbors.config]
    neighbor-address = "10.0.255.2"
    peer-as = 65002
```

### 7.2 Route Reflector com dois clientes

```toml
[global.config]
  router-id = "192.168.0.1"
  as = 65000

[[neighbors]]
  [neighbors.config]
    neighbor-address = "192.168.10.2"
    peer-as = 65000
  [neighbors.route-reflector.config]
    route-reflector-client = true
    route-reflector-cluster-id = "192.168.0.1"

[[neighbors]]
  [neighbors.config]
    neighbor-address = "192.168.10.3"
    peer-as = 65000
  [neighbors.route-reflector.config]
    route-reflector-client = true
    route-reflector-cluster-id = "192.168.0.1"
```

Mais contexto em [route-reflector.md](route-reflector.md) e, para Route
Server, em [route-server.md](route-server.md).

### 7.3 Servidor multi-homed a um CPE, com `fib` em modo `enforce`

Esta é a topologia central desta entrega: um servidor Linux ligado a um
único CPE por duas interfaces físicas, cada uma com sub-rede e gateway
próprios, e uma única sessão BGP em eBGP multihop entre endereços de
loopback, para que a queda de uma interface de dados não derrube a sessão
nem retire as rotas que sairiam pela outra interface.

```toml
[global.config]
  as = 65001
  router-id = "192.0.2.20"
  port = -1

[[neighbors]]
  [neighbors.config]
    neighbor-address = "192.0.2.10"
    peer-as = 65002
  [neighbors.transport.config]
    local-address = "192.0.2.20"
  [neighbors.ebgp-multihop.config]
    enabled = true
    multihop-ttl = 3
  # Graceful restart fica desligado neste vizinho: o fib trata caminho
  # obsoleto (stale) como ausente (seções 4.5 e 9).
  [[neighbors.afi-safis]]
    [neighbors.afi-safis.config]
      afi-safi-name = "ipv4-unicast"

[[defined-sets.prefix-sets]]
  prefix-set-name = "customers"
  [[defined-sets.prefix-sets.prefix-list]]
    ip-prefix = "198.51.100.0/24"
    masklength-range = "24..32"

[[defined-sets.bgp-defined-sets.community-sets]]
  community-set-name = "prefer-wan2"
  community-list = ["65002:200"]

[fib]
  enabled = true
  mode = "enforce"
  protocol = 200
  metric = 20
  protected-prefixes = ["10.10.0.0/16"]

  [fib.health]
    interval = "1s"
    failures = 3
    successes = 3
    hold-down = "10s"
    max-hold-down = "5m"

  [fib.startup]
    eor-timeout = "30s"
    max-retention = "2m"

  [[fib.exit-paths]]
    name = "wan1"
    interface = "eth1"
    gateway = "10.0.1.1"

  [[fib.exit-paths]]
    name = "wan2"
    interface = "eth2"
    gateway = "10.0.2.1"

  # A rota até a loopback do CPE sustenta a própria sessão BGP.
  [[fib.anchors]]
    name = "cpe-loopback"
    prefix = "192.0.2.10/32"
    exit-paths = ["wan1", "wan2"]

  [[fib.rules]]
    name = "tagged-for-wan2"
    priority = 10
    table = 100
    exit-paths = ["wan2", "wan1"]
    [fib.rules.conditions.bgp-conditions.match-community-set]
      community-set = "prefer-wan2"

  [[fib.rules]]
    name = "customers"
    priority = 20
    table = 100
    exit-paths = ["wan1", "wan2"]
    [fib.rules.conditions.match-prefix-set]
      prefix-set = "customers"

  [[fib.rules]]
    name = "everything-else"
    priority = 1000
    table = "main"
    exit-paths = ["wan1", "wan2"]

  [[fib.traffic-rules]]
    name = "lan"
    priority = 1010
    table = 100
    from = "172.16.0.0/16"

  [fib.default-route]
    mode = "table"
    table = 100
    exit-paths = ["wan1", "wan2"]
```

O mesmo arquivo, com comentários adicionais sobre os pré-requisitos de rede
no CPE, está em [`docs/examples/fib.toml`](../examples/fib.toml), e o modelo
de rede completo (eBGP multihop entre loopbacks, alternativas sem loopback
no CPE, e o caso degradado de sessão em um único salto) está detalhado na
seção "Network design" de [`docs/sources/linux-fib.md`](linux-fib.md).

### 7.4 `fib` em modo `observe`: validar antes de aplicar

A mesma configuração da seção 7.3, trocando apenas `mode = "enforce"` por
`mode = "observe"` (ou omitindo `mode`, que é o padrão), registra em log
cada decisão (regra escolhida, tabela, caminhos de saída) sem tocar no
kernel. É o modo recomendado para validar as regras em produção antes de
ativar a escrita (seção 10.3).

### 7.5 RPKI em conjunto com Route Server

```toml
[global.config]
  as = 64512
  router-id = "10.0.255.254"

[[neighbors]]
  [neighbors.config]
    peer-as = 65001
    neighbor-address = "10.0.255.1"
  [neighbors.route-server.config]
    route-server-client = true

[[neighbors]]
  [neighbors.config]
    peer-as = 65002
    neighbor-address = "10.0.255.2"
  [neighbors.route-server.config]
    route-server-client = true
  [neighbors.apply-policy.config]
    import-policy-list = ["AS65002-IMPORT-RPKI"]

[[rpki-servers]]
  [rpki-servers.config]
    address = "210.173.170.254"
    port = 323

[[policy-definitions]]
  name = "AS65002-IMPORT-RPKI"
  [[policy-definitions.statements]]
    name = "statement1"
    [policy-definitions.statements.conditions.bgp-conditions]
      rpki-validation-result = "invalid"
    [policy-definitions.statements.actions]
      route-disposition = "reject-route"
```

Detalhes e saídas de verificação em [rpki.md](rpki.md).

---

## 8. Rota default, rotas-âncora e proteção administrativa

### 8.1 Tratamento da rota default (`[fib.default-route]`)

A rota `0.0.0.0/0` recebida por BGP **nunca** é capturada pelas regras
genéricas de `fib.rules` (seção 5.3); seu tratamento é sempre explícito em
`fib.default-route`:

| Campo | Padrão | Restrição |
| --- | --- | --- |
| `mode` | `ignore` | `ignore`, `table` ou `main` |
| `table` | — | Obrigatório em `mode = "table"`; não pode ser `"main"` (use `mode = "main"` para isso) |
| `exit-paths` | — | Obrigatório quando `mode != "ignore"` |
| `replace-external` | `false` | Só é válido com `mode = "main"` |
| `conditions` | — | Opcional; mesma sintaxe das regras (seção 5.3); se presente, só instala a default quando a condição é satisfeita |

- **`ignore`** (padrão): a default anunciada é descartada pelo `fib`; o
  roteamento do servidor não é afetado.
- **`table`**: instala a default apenas na tabela indicada, usada somente
  pelo tráfego que uma regra de tráfego direcionar para lá (seção 6.2).
  **É a opção mais segura** para não arriscar a conectividade de
  administração do próprio servidor.
- **`main`**: altera a rota default efetiva do servidor. Uma rota default
  alheia com métrica diferente continua coexistindo (vence a de menor
  métrica); uma rota default alheia com a **mesma métrica** é tratada como
  conflito (seção 6.3) e não é sobrescrita, **a menos que
  `replace-external = true`**, caso em que ela é substituída e restaurada
  automaticamente quando o BGP retira a default ou o `gobgpd` para
  (seção 9.6). Após um `kill -9`, a rota original substituída **não** é
  restaurada (ela só é guardada em memória, não em disco) — ver seção 11.

### 8.2 Protegendo o acesso administrativo

Para não perder o acesso de gerência ao servidor:

1. Garanta que a rede de gerência tenha uma rota mais específica que a
   default na tabela `main` (normalmente a rota conectada já cumpre isso).
2. Liste o prefixo de gerência em `fib.protected-prefixes`: nenhuma rota de
   BGP igual ou mais específica poderá ocupá-lo na tabela `main`
   (seção 5.3/6.3).
3. Prefira `fib.default-route.mode = "table"` até validar as regras em modo
   `observe` (seção 7.4) em produção.
4. Se usar `mode = "main"`, não habilite `replace-external` sem terceirizar
   o acesso administrativo para fora da default (por exemplo, por uma rota
   estática mais específica ou uma interface fora de banda).

### 8.3 Rotas-âncora (`[[fib.anchors]]`)

Uma âncora é uma rota independente do BGP, mantida pelo próprio `fib`, para
que a sessão BGP sobreviva à falha de um dos caminhos de dados. O caso de
uso canônico é a rota até o endereço de loopback do CPE, usado como
`neighbor-address` numa sessão eBGP multihop (seção 7.3): sem essa rota
sendo monitorada e trocada de caminho de forma independente do BGP, a
própria sessão ficaria vulnerável à falha silenciosa de um link.

| Campo | Padrão | Restrição |
| --- | --- | --- |
| `name` | — | Obrigatório, único |
| `prefix` | — | Obrigatório; prefixo IPv4 mascarado, diferente de `0.0.0.0/0` |
| `table` | `main` | `"main"` ou um número de tabela |
| `exit-paths` | — | Obrigatório; lista ordenada de caminhos de saída |

Comportamento (`pkg/fib/manager.go`, `syncAnchors`):

- Âncoras são instaladas **antes** de os vizinhos BGP serem iniciados
  (`Manager.Start`), e o processo aguarda até `fib.anchor-wait` (padrão
  `10s`) por um primeiro veredito de saúde de cada caminho de saída, para
  que a âncora já suba pelo caminho preferido em vez do primeiro a
  responder.
- Enquanto houver pelo menos um caminho de saída saudável na lista, a
  âncora usa o primeiro saudável.
- Se **todos** os caminhos de saída da âncora ficarem indisponíveis, ela
  **permanece no último caminho ativo** (não é removida), justamente para
  não derrubar de propósito uma sessão que ainda possa estar funcionando
  por aquele caminho.
- Uma âncora nunca é removida pela perda de sessão BGP nem pela
  reconciliação de reinício; só muda por mudança de saúde dos caminhos ou
  na parada limpa do processo (seção 9.6).
- Prefixos de âncoras são protegidos automaticamente, como os
  `protected-prefixes`: uma rota de BGP igual ou mais específica, na mesma
  tabela da âncora, não é instalada.
- Uma `ip rule` de isenção é criada automaticamente para cada âncora,
  garantindo que o tráfego até o prefixo da âncora sempre consulte a tabela
  da âncora, com prioridade acima das regras de tráfego manuais
  (seção 6.2).

---

## 9. Failover, monitoramento de caminhos e reconciliação

### 9.1 Saúde de um caminho de saída (`[fib.health]`)

| Campo | Padrão | Restrição |
| --- | --- | --- |
| `interval` | `1s` | Tempo entre verificações de cada caminho; mínimo `100ms` |
| `failures` | `3` | Falhas consecutivas de sonda para considerar o caminho indisponível |
| `successes` | `3` | Sucessos consecutivos para voltar a usar o caminho |
| `hold-down` | `10s` | Tempo de estabilidade exigido após uma falha, antes de voltar a usar o caminho |
| `max-hold-down` | `5m` | Teto do `hold-down`, que **dobra** a cada nova falha ocorrida antes de completar `max-hold-down` de estabilidade desde a última recuperação; um período estável de `max-hold-down` volta o `hold-down` ao valor base |

Mecanismo de detecção (`pkg/fib/health.go`, `pkg/fib/monitor.go`,
`pkg/fib/probe_linux.go`):

- **Estado do link**: obtido por eventos do netlink
  (`netlink.LinkSubscribeWithOptions`); a perda de portadora derruba o
  caminho **imediatamente**, sem esperar `failures` tentativas.
- **Alcançabilidade do gateway** (`probe = "arp"`, padrão): uma requisição
  ARP é enviada ao gateway a cada `interval`; falta de resposta conta como
  falha. Isso prova o caminho até o gateway, não além dele.
- **Somente link** (`probe = "link"`): para interfaces sem ARP (ex.: PPP);
  só o estado do link é considerado.

Esse desenho atende diretamente ao requisito de distinguir "interface
ativa" (link up) de "caminho realmente alcançável" (gateway responde), e de
evitar oscilação excessiva por meio dos limiares de `failures`/`successes`
e do `hold-down` progressivo.

### 9.2 Os três eventos e seus efeitos

| Evento | O que acontece no BGP | Efeito no kernel |
| --- | --- | --- |
| Retirada explícita de uma rota (ou mudança do melhor caminho) | Só aquele destino muda | A rota correspondente é removida ou movida de tabela/caminho imediatamente, sem retenção artificial |
| Perda da sessão BGP | Todas as rotas daquele vizinho saem da RIB | Todas as rotas dinâmicas (vindas de BGP) desse processo são removidas; âncoras e regras de tráfego permanecem |
| Caminho de saída indisponível | Nada muda no BGP | As rotas que usavam aquele caminho são reapontadas para o próximo caminho saudável da sua regra; sem nenhum caminho saudável, a rota é removida do kernel (a âncora correspondente, se houver, é a exceção: ela fica no último caminho ativo, seção 8.3) |

Um caminho que volta a ficar saudável só é usado de novo após `hold-down`
(seção 9.1); por isso o retorno ao caminho preferido não é instantâneo por
desenho.

### 9.3 Reconciliação na inicialização

Ao iniciar com `fib.mode = "enforce"` (`pkg/fib/manager.go`, `begin`/`Start`):

1. O inventário de rotas já existentes com o número de protocolo configurado
   é levantado (`ownedRoutes`).
2. As âncoras e as `ip rule` são (re)instaladas.
3. Rotas herdadas de uma execução anterior são **mantidas** até a
   convergência do BGP, para minimizar a janela sem encaminhamento.
4. A convergência é considerada atingida no primeiro dos três eventos:
   - toda sessão BGP estabelecida enviar End-of-RIB para IPv4 unicast;
   - `fib.startup.eor-timeout` (padrão `30s`) após a primeira sessão
     estabelecer, para o caso de o par não enviar End-of-RIB (isso só
     ocorre com Graceful Restart negociado — o próprio GoBGP só envia
     End-of-RIB nessa condição);
   - `fib.startup.max-retention` (padrão `2m`) após o início do processo,
     mesmo sem nenhuma sessão estabelecida.
5. Rotas herdadas que nenhuma regra confirmou são removidas nesse momento.

### 9.4 Resync periódico

A cada `fib.resync-interval` (padrão `30s`), o `fib` compara seu estado
interno com o kernel: rotas apagadas ou alteradas por outro processo são
reinstaladas; rotas e regras que o `fib` possui mas não deveriam mais
existir são removidas (depois da reconciliação inicial); conflitos e
operações que falharam anteriormente são tentados de novo.

### 9.5 Comportamento em crash

Se o processo morrer (`kill -9`, falha de hardware), as rotas e regras já
instaladas **permanecem no kernel**, mas sem failover algum até o `gobgpd`
voltar a rodar — nesta versão não há reserva de rota com métrica mais alta
instalada previamente no kernel nem objetos nexthop do kernel para esse
fim (ver seção 11). Recomenda-se `Restart=always` no systemd (seção 1.5)
para minimizar essa janela.

### 9.6 Parada limpa

Um `SIGTERM`/`SIGINT` recebido pelo `gobgpd` remove, nesta ordem, tudo o que
tem o número de protocolo configurado: as rotas dinâmicas de BGP, depois as
regras de tráfego, depois as âncoras (`pkg/fib/manager.go`, `cleanup`). Uma
rota default de terceiros substituída por `replace-external = true`
(seção 8.1) é restaurada nesse momento. Objetos de outros donos nunca são
tocados.

---

## 10. Diagnóstico, troubleshooting e logs

### 10.1 Opções de inicialização relevantes para diagnóstico (`gobgpd`)

| Opção | Padrão | Uso |
| --- | --- | --- |
| `-d`, `--dry-run` | — | Valida a configuração (BGP, Kafka, fib) sem iniciar nada |
| `-l`, `--log-level` | `info` | `debug` imprime a configuração carregada em `--dry-run` |
| `-p`, `--log-plain` | JSON | Log em texto plano em vez de JSON |
| `--disable-stdlog` | — | Desativa a saída padrão de log |
| `--api-hosts` | `:50051` | Endereço(s) de escuta da API gRPC de gerência |
| `--pprof-host` | `localhost:6060` | Endpoint de `pprof` e métricas Prometheus |
| `--metrics-path` | `/metrics` | Caminho das métricas Prometheus; vazio desativa |
| `--pprof-disable` | — | Desativa `pprof`; **não** desativa as métricas se `--metrics-path` estiver definido |
| `--route-refresh-stale-time` | `300` s | Retenção máxima de Enhanced Route Refresh incompleto; `0` desativa |
| `--sdnotify` | — | Protocolo `sd_notify` do systemd |
| `--kafka-control-host` | — | Listener de gestão de captura Kafka, exige arquivo de configuração |

### 10.2 Diagnóstico de sessões e rotas BGP (`gobgp`)

```sh
gobgp neighbor                       # resumo de todos os vizinhos
gobgp neighbor <endereço>            # estado detalhado, capacidades, contadores
gobgp neighbor <endereço> adj-in     # rotas recebidas (antes da política)
gobgp neighbor <endereço> adj-out    # rotas enviadas
gobgp neighbor <endereço> local      # RIB local após a política
gobgp global rib                     # RIB global
gobgp global rib summary             # contagem por família
gobgp global policy import           # política global de importação
gobgp neighbor <endereço> policy import   # política de importação do vizinho
gobgp policy                         # políticas definidas
gobgp monitor global                 # eventos de rota em tempo real
gobgp monitor neighbor               # eventos de peer em tempo real
gobgp monitor adj-in                 # eventos de adj-rib-in em tempo real
gobgp rpki server                    # estado das sessões RTR
gobgp rpki table <endereço>          # tabela de ROA recebida
gobgp vrf                            # VRFs configuradas
gobgp bmp                            # estações BMP
gobgp keychain                       # chaveiros TCP-AO
gobgp log-level debug                # muda o nível de log em tempo de execução
```

Exemplos completos de saída para cada comando estão em
[`docs/sources/cli-operations.md`](cli-operations.md) e
[`docs/sources/cli-command-syntax.md`](cli-command-syntax.md). O cliente
aceita `-u`/`--host`, `-p`/`--port` (API gRPC, padrão `127.0.0.1:50051`),
`-j`/`--json`, e suporte a TLS (`--tls`, `--tls-ca-file`,
`--tls-client-cert-file`, `--tls-client-key-file`) e a socket Unix
(`--target unix:///caminho`).

### 10.3 Diagnóstico da extensão `fib`

**Não existe, nesta versão, comando `gobgp` nem chamada gRPC para consultar
o estado do `fib`.** Isso é uma limitação real, não uma omissão deste
manual (ver seção 11). As únicas fontes de observação são:

1. **Logs**, com `Topic=Fib` (formato texto com `--log-plain`, campos entre
   aspas quando têm espaço):

   ```text
   level=INFO msg="fib started" Topic=Fib Mode=enforce Rules=3 Anchors=1 TrafficRules=1
   level=INFO msg="fib decision" Topic=Fib Mode=enforce Prefix=198.51.100.0/24 Action=install Rule=customers Reason=rule Table=100 ExitPaths="[wan1 wan2]" Nexthop=10.0.255.1
   level=INFO msg="fib route" Topic=Fib Prefix=198.51.100.0/24 Route="198.51.100.0/24 via 10.0.1.1 dev wan1 table 100 metric 20" Status=installed
   level=WARN msg="fib exit path is not usable" Topic=Fib ExitPath=wan1 Usable=false Reason="gateway unreachable" Detail="no ARP reply from 10.0.1.1 on wan1"
   level=INFO msg="fib exit path is usable" Topic=Fib ExitPath=wan1 Usable=true Reason=usable
   level=WARN msg="fib anchor" Topic=Fib Anchor=cpe-loopback Route="192.0.2.10/32 via 10.0.2.1 dev wan2 table 254 metric 20" Status="kept on the last active exit path"
   level=INFO msg="fib reconciled the routes of the previous run" Topic=Fib Reason="End-of-RIB received from every established peer" Removed=1
   level=WARN msg="fib kernel operation failed" Topic=Fib Owner=198.51.100.0/24 Operation=replace Route="..." Error="..."
   ```

   Os valores de `Status` possíveis são: `installed`, `installed in place of
   a foreign route`, `kept on the last active exit path`, `kept from the
   previous run`, `observed`, `ignored`, `no usable exit path`,
   `protected prefix`, `conflict`, `error`.

2. **Métricas Prometheus** (`gobgp_fib_*`, expostas em `--pprof-host` sob
   `--metrics-path`):

   | Métrica | Tipo | Significado |
   | --- | --- | --- |
   | `gobgp_fib_exit_path_usable{exit_path}` | gauge | `1` se o caminho está saudável |
   | `gobgp_fib_exit_path_changes_total{exit_path}` | counter | Mudanças de estado do caminho |
   | `gobgp_fib_destinations{status}` | gauge | Destinos por status |
   | `gobgp_fib_anchors{status}` | gauge | Âncoras por status |
   | `gobgp_fib_traffic_rules{status}` | gauge | Regras e isenções por status |
   | `gobgp_fib_reconciled` | gauge | `1` depois de reconciliar as rotas da execução anterior |
   | `gobgp_fib_kernel_errors_total` | counter | Operações de kernel que falharam, incluindo conflitos |
   | `gobgp_fib_routes_repaired_total` | counter | Rotas próprias reinstaladas após alteração externa |

3. **O próprio kernel**, já que o `fib` só manipula objetos com o seu número
   de protocolo:

   ```sh
   ip route show table all proto 200
   ip rule show | grep "proto 200"
   ```

   (ajuste `200` para o valor de `fib.protocol`, se customizado).

### 10.4 Fluxo de troubleshooting sugerido

1. **A configuração carrega?** `gobgpd -f arquivo -d -l debug`. Erros de
   `fib` aparecem prefixados por `fib:` ou `fib rule <nome>:`/`fib anchor
   <nome>:`/`fib exit path <nome>:`; erros de `kafka` por `kafka:`/`kafka
   sink <nome>:`.
2. **A sessão BGP sobe?** `gobgp neighbor <endereço>`; veja `BGP state` e os
   contadores de mensagens. Se não subir, confira AS, endereço, TTL/GTSM
   (seção 4.4) e, no caso de sessão multihop, se a rota-âncora até o
   endereço do vizinho existe (`ip route get <endereço-do-vizinho>`).
3. **A rota chega na RIB?** `gobgp neighbor <endereço> adj-in` (antes da
   política) e `gobgp neighbor <endereço> local`/`gobgp global rib` (depois
   da política). Se a rota sumir aqui, o problema é de política BGP
   (seção 5.1), não de `fib`.
4. **O `fib` decide o que eu esperava?** Rode em `mode = "observe"`
   (seção 7.4) e acompanhe `msg="fib decision"` no log para o prefixo em
   questão: confira `Rule`, `Table` e `ExitPaths`. Se `Reason` for
   `"no matching rule"`, nenhuma regra casou — normalmente falta uma regra
   "pega-tudo" de prioridade alta.
5. **A rota está no kernel?** Em modo `enforce`,
   `ip route show table <tabela> proto <protocolo>` e
   `msg="fib route"`/`Status` no log. `Status=conflict` indica que outro
   dono já ocupa a chave (tabela+prefixo+métrica); `Status="no usable exit
   path"` indica que nenhum caminho de saída da regra está saudável.
6. **O tráfego usa a tabela certa?** `ip rule show`, e
   `ip route get <destino> from <origem> iif <interface>` para simular a
   decisão do kernel com policy routing.
7. **Por que um caminho está marcado como indisponível?**
   `msg="fib exit path is not usable"` traz `Reason` (`link down` ou
   `gateway unreachable`) e, quando aplicável, `Detail` com o erro da sonda.

### 10.5 Métricas gerais do BGP

As métricas Prometheus do núcleo BGP (`bgp_routes_received`,
`bgp_sent_discarded_total` etc.), independentes do `fib`, estão descritas em
[`docs/sources/metrics.md`](metrics.md), incluindo como fazer o scrape e um
modelo de dashboard Grafana.

---

## 11. Limitações conhecidas e cuidados operacionais

Esta seção reúne limitações confirmadas no código, para que nada seja usado
além do que foi de fato implementado.

### 11.1 Da extensão `fib`

- **Somente IPv4 unicast.** `fib.rules[].family` só aceita
  `"ipv4-unicast"`; não há suporte a IPv6 nesta versão (a arquitetura foi
  desenhada para não impedir essa evolução, mas ela ainda não existe).
- **Sem API nem CLI de consulta.** Todo o estado do `fib` só é observável
  por log e métricas Prometheus (seção 10.3); não há comando `gobgp` nem
  chamada gRPC equivalente a `gobgp neighbor`/`gobgp global rib` para o
  `fib`.
- **Sem recarga a quente.** Mudar `[fib]` exige reiniciar o processo
  (seção 2.4); um `SIGHUP` apenas registra um aviso e mantém a configuração
  ativa.
- **Um único caminho de saída instalado por vez.** Não há ECMP entre
  caminhos de saída nem rota de reserva com métrica mais alta já instalada
  no kernel: o failover é feito pelo processo trocando a rota, não pelo
  próprio kernel. Em caso de `kill -9`, não há failover até o `gobgpd`
  voltar a rodar (seção 9.5).
- **A sonda `arp` só prova o caminho até o gateway**, não além dele; um CPE
  que responda ARP mas não encaminhe pacotes não é detectado como falho.
- **A rota default original, quando substituída com `replace-external =
  true`, é guardada apenas em memória.** Um `kill -9` não a restaura
  (seção 8.1); apenas a parada limpa (`SIGTERM`/`SIGINT`) o faz.
- **Regras de tráfego exigem kernel Linux 4.17+** (atributo de protocolo em
  `ip rule`); sem elas, rotas funcionam em kernels mais antigos.
- **Trocar `fib.protocol` entre execuções não limpa objetos do número
  anterior.** As rotas e regras do protocolo antigo ficam no kernel até
  serem removidas manualmente.
- **A engine consome o melhor caminho único da RIB global**, mesmo quando
  `global.use-multiple-paths` (ECMP do BGP) está habilitado; o `fib` não
  instala múltiplos caminhos BGP recebidos por ECMP, apenas a lista
  ordenada de caminhos de saída da regra que casou.
- Um plano de testes com cenários que exigem equipamento real, escala,
  outros kernels ou execução prolongada — ainda não executados — está em
  `planning/r11-fib-test-plan.md` e na
  [issue #23](https://github.com/nishisan-dev/n-gobgp/issues/23) do
  repositório.

### 11.2 Gerais do projeto (não específicas do `fib`)

- `[global]`, `[zebra]`, `[[rpki-servers]]`, `[[bmp-servers]]`, `[[vrfs]]` e
  `[[mrt-dump]]` não são reprocessados por `SIGHUP`/recarga automática;
  mudanças nessas seções exigem reiniciar o `gobgpd` (seção 2.4).
- A API gRPC de gerência (`--api-hosts`, padrão `:50051`) não tem
  autenticação própria além de TLS opcional; um cliente que a alcance pode
  adicionar/remover vizinhos e rotas e parar o daemon. Restringir o acesso
  de rede a essa porta é responsabilidade operacional.
- `eBGP Multihop` e `TTL Security` são mutuamente exclusivos no mesmo
  vizinho (seção 4.4).
- BFD nativo é de sessão única (RFC 5881), acoplado ao vizinho BGP; não
  cobre, por si só, múltiplos gateways de uma sessão multihop (por isso a
  extensão `fib` tem seu próprio monitor de caminhos, independente do BFD).
- Apenas formatos `toml`, `yaml` e `json` são aceitos para o arquivo de
  configuração (confirmado em `pkg/config/oc/util.go`); nenhum outro
  formato é reconhecido por este fork.
- A compilação cruzada é verificada pelo CI apenas para `amd64`; o
  `required` do GitHub Actions deste repositório cobre essa e as demais
  checagens descritas em `.github/workflows/ci.yml`.

### 11.3 Cuidados operacionais recomendados

- Valide toda mudança de configuração com `gobgpd -f arquivo -d` antes de
  aplicar (seção 1.3).
- Ao adotar `fib.mode = "enforce"` pela primeira vez, rode antes em
  `mode = "observe"` por um período e compare as decisões logadas com o
  esperado (seção 7.4), como também recomendado no item 3 de
  `planning/r11-fib-test-plan.md`.
- Proteja sempre a rede de gerência com `fib.protected-prefixes`
  (seção 8.2) antes de habilitar `fib.default-route.mode = "main"`.
- Rode o `gobgpd` sob um supervisor com reinício automático
  (`Restart=always` no systemd) quando usar `fib.mode = "enforce"`, para
  minimizar a janela sem failover após uma falha do processo (seção 9.5).
- Ao usar `[[fib.traffic-rules]]`, confirme a versão do kernel (`uname -r`,
  seção 1.1) antes de depender delas em produção.
- Se o Zebra (FRR/Quagga) e o `fib` forem usados no mesmo servidor, use
  tabelas separadas; o `--dry-run` avisa quando as duas apontam para a
  tabela `main` (seção 5.4), mas não impede essa configuração.
