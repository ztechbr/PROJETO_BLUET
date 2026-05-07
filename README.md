# Servidor API — BlueSensores (UTFPR)

Este repositório é uma API em Python com Flask para o projeto de sensores da UTFPR. Ela expõe REST com documentação Swagger, grava leituras em PostgreSQL na tabela `leituras` e ainda oferece consulta por SOAP 1.1 no mesmo caminho `/soap`, o que ajuda em testes com o app Android e em integrações mais antigas.

O fluxo principal é: `POST /leituras` recebe JSON e persiste no banco. Para buscar dados com filtros, você pode usar `GET /leituras` (JSON, com token se o servidor estiver configurado assim) ou o atalho `GET /soap?format=json|xml` e o POST SOAP clássico, que não usam o mesmo `API_TOKEN` do REST. Os filtros segem a mesma ideia em todas as formas de consulta.

Convenção sugerida para o repositório no GitHub: nome `Servidor_API_Projeto_BlueSensores_UTFPR`. Para renomear um repo que já existe: *Settings → General → Repository name*.

O que está aqui serve bem para desenvolvimento e laboratório. Para colocar na internet de verdade, prevê HTTPS, rever o modo de servidor (WSGI), política de token e tudo o que o seu ambiente de produção exige.

## Pré-requisitos

Python 3.10 ou superior, PostgreSQL com a tabela criada a partir de `scripts_bd/create_table.sql`. Esse script já traz o schema completo (Bluetooth, grandezas elétricas, RSSI, sensor, etc.). Se você herdar um banco muito antigo sem alguma coluna, no final do mesmo arquivo existe um bloco `ALTER TABLE` comentado que pode ser descomentado com cuidado só para as colunas que faltam.

A máquina onde a API roda precisa ser alcançável pelo celular ou emulador (mesma rede Wi‑Fi, IP da LAN, ou algum túnel se for o caso).

## Banco e `.env`

Copie o exemplo e edite:

```bash
cp .env.example .env
```

A conexão pode ser feita de duas maneiras:

1. URL única: `DATABASE_URL=postgresql://usuario:senha@host:5432/nome_do_banco`
2. Variáveis separadas: `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`

Outras variáveis úteis:

| Variável | Função |
|----------|--------|
| `PORT` | Porta HTTP da API (padrão 8001). |
| `API_TOKEN` | Se vier preenchido, `GET` e `POST /leituras` exigem o mesmo segredo no header. Não vale para `/soap`, `/health` nem `/apidocs`. |
| `SOAP_PUBLIC_URL` | URL pública do SOAP (ex.: `https://seu-dominio/soap`). Ajusta o endereço no WSDL. |
| `SOAP_NAMESPACE` | Namespace XML do WSDL se quiser fixar manualmente; caso contrário pode derivar de `SOAP_PUBLIC_URL`. |
| `SWAGGER_UI_AUTH_TTL_HOURS` | Opcional: lembrete visual no Swagger após autorizar (horas; 0 desliga a data). |

O SOAP depende de Spyne (em alguns ambientes o pip usa Git) e de `lxml`. No Docker, o Git entra só na fase de build se precisar puxar dependência.

Mais detalhes e comentários estão no próprio `.env.example`.

## Como subir localmente

```bash
cd Servidor_API_Projeto_BlueSensores_UTFPR
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python app.py
```

Por padrão o processo escuta em `0.0.0.0:8001`, então outro equipamento na rede alcança pelo IP da máquina.

## Visão rápida das rotas REST e SOAP

| Método | Caminho | Para quê serve |
|--------|---------|----------------|
| GET | `/health` | Ping simples (`{"status":"ok"}`). |
| GET | `/leituras` | Lista com filtros (pelo menos um filtro obrigatório). |
| POST | `/leituras` | Insere leitura em JSON. |
| GET | `/soap` | Depende da query: `?wsdl`, ou `?format=json|xml` com filtros, ou ajuda em JSON se não mandar filtro. Sem `API_TOKEN`. |
| POST | `/soap` | Chamada SOAP 1.1, operação `listarLeituras`. Sem `API_TOKEN`. |

Quem quer brincar no navegador com parâmetros prontos usa o Swagger em `/apidocs`. JSON direto no app ou em script costuma ir por `/leituras`. Quem precisa de WSDL e XML continua no POST `/soap`. Gravação nova de leitura é só pelo REST (`POST /leituras`); leitura filtrada pode ser REST ou SOAP.

## Swagger (`/apidocs`)

Com o servidor no ar, abra `http://<host>:<porta>/apidocs` (por exemplo `127.0.0.1:8001`). Lá estão `GET /leituras`, `POST /leituras` e `GET /health`. O SOAP não aparece nessa UI; use as URLs da seção seguinte.

Se existir `API_TOKEN` no servidor, use **Authorize** antes de testar `/leituras`. O Swagger costuma mandar só o texto do segredo no header `Authorization` (sem a palavra `Bearer`). A API aceita as três formas: só o token nesse header, `Bearer <token>`, ou `X-API-Key: <token>`.

Fluxo típico: expandir a operação, **Try it out**, preencher, **Execute**. Abaixo aparece o `curl` e a resposta. Se a página ficar estranha após atualizar o servidor, faça um recarregar forçado no navegador.

## REST e `API_TOKEN`

Com token configurado no `.env`, toda chamada a `GET /leituras` e `POST /leituras` precisa trazer o mesmo valor que você definiu lá. Formas aceitas:

* `Authorization: Bearer <API_TOKEN>`
* `Authorization: <API_TOKEN>` (como muitos clientes geram ao digitar só o segredo)
* `X-API-Key: <API_TOKEN>`

Se `API_TOKEN` estiver vazio, essas rotas ficam abertas (útil em laboratório). `/soap`, `/health` e `/apidocs` não dependem desse segredo.

## SOAP e `/soap`

No mesmo path `/soap` você tem três ideias diferentes:

**WSDL** para importar em cliente gerado ou SoapUI: `GET /soap?wsdl`.

**GET com JSON ou XML** para testar filtro rápido no navegador, com os mesmos parâmetros de query do `GET /leituras`, mais `format=json` ou `format=xml`. Exemplo: `/soap?format=json&codplantacao=PLANTDEMO`.

**POST SOAP 1.1** com envelope XML na operação `listarLeituras`.

O `GET` alternativo em JSON/XML é um facilitador; o contrato oficial continua sendo o WSDL + POST. Ajustes em `SOAP_PUBLIC_URL` ou `SOAP_NAMESPACE` mudam endereço e namespace no XML, não “outro protocolo”.

Filtros: é preciso informar pelo menos um entre `codplantacao`, `dataleit_inicio` e `dataleit_fim`; `limit` e `offset` são opcionais. Nenhuma dessas chamadas SOAP usa `API_TOKEN`.

O namespace que entra no XML (`xmlns:tns`) tem que bater com o `targetNamespace` do WSDL que você baixou na própria máquina. Se só `SOAP_PUBLIC_URL` estiver definido e `SOAP_NAMESPACE` vazio, o projeto deriva algo no formato `{esquema}://{host}/leituras`. Sem nenhum dos dois, o README de exemplo pode citar `http://utfpr.edu.br/bluesensores/leituras`, mas confira sempre o arquivo real que o servidor serve.

Referência rápida:

| Uso | Onde |
|-----|------|
| WSDL | `https://<host>/soap?wsdl` |
| JSON/XML via GET | `/soap?format=json` ou `format=xml` com os mesmos parâmetros de query do REST |
| POST clássico | corpo SOAP 1.1, `SOAPAction: listarLeituras`, `Content-Type: text/xml; charset=utf-8` |

Teste no navegador, por exemplo: `http://127.0.0.1:8001/soap?format=json&codplantacao=PLANTDEMO`. Só ver o XML do contrato: mesma URL com `?wsdl`.

No POST, o bloco `filtro` aceita campos espelhando a query REST. Sem filtro válido vem SOAP Fault, no mesmo espírito de erro que o `GET /leituras` sem condição.

Exemplo de envelope (troque datas, plantação e `xmlns:tns` se o seu WSDL pedir outro):

```xml
<?xml version="1.0" encoding="UTF-8"?>
<soap11env:Envelope xmlns:soap11env="http://schemas.xmlsoap.org/soap/envelope/">
  <soap11env:Body>
    <tns:listarLeituras xmlns:tns="http://utfpr.edu.br/bluesensores/leituras">
      <tns:filtro>
        <tns:codplantacao>PLANTDEMO</tns:codplantacao>
        <tns:dataleit_inicio>2026-05-01</tns:dataleit_inicio>
        <tns:dataleit_fim>2026-05-31</tns:dataleit_fim>
        <tns:limit>100</tns:limit>
        <tns:offset>0</tns:offset>
      </tns:filtro>
    </tns:listarLeituras>
  </soap11env:Body>
</soap11env:Envelope>
```

Exemplo salvando esse XML como `request.xml`:

```bash
curl -s -X POST "http://127.0.0.1:8001/soap" \
  -H "Content-Type: text/xml; charset=utf-8" \
  -H "SOAPAction: listarLeituras" \
  --data-binary @request.xml
```

### Script de terminal: `testes/soaptest.py`

Serve para ver WSDL, montar POST, inspecionar Fault e usar o GET atalho. Lê `.env` (via `python-dotenv`); se existir `SOAP_PUBLIC_URL`, usa como base; senão prefere `http://127.0.0.1:8001`.

```bash
python3 testes/soaptest.py --help
python3 testes/soaptest.py
```

Sem subcomando ele imprime ajuda com exemplos. Os subcomandos são:

* `wsdl` — baixa e resume o contrato (`--full` mostra o XML inteiro).
* `get` — chama `/soap?format=json|xml` com filtros (não é o SOAP pesado).
* `call` — POST `listarLeituras`; `--show-request` mostra o envelope; `--tns` só se precisar forçar namespace.

Globais úteis: `--base-url`, `--timeout`, `--insecure-tls` em HTTPS com certificado ruim.

Exemplo de período por plantação (ajuste datas para o seu caso):

```bash
python3 testes/soaptest.py call \
  --base-url http://127.0.0.1:8001 \
  --codplantacao PLANTDEMO \
  --dataleit-inicio 2026-05-01 \
  --dataleit-fim 2026-05-07 \
  --show-request
```

Mesmo que `API_TOKEN` exista no `.env`, o `/soap` continua sem ele; o segredo só amarra o REST `/leituras`.

### Scripts REST: `testes/resttest_get.py` e `testes/resttest_post.py`

Imprimem URL, status, cabeçalhos relevantes e corpo (inclusive `error`, `detail`, `missing` quando der problema). Também carregam `.env` e, se houver token, já enviam.

O valor do token não é “gerado” pelo script: você coloca em `API_TOKEN=` no servidor e repete o mesmo valor no cliente como `Bearer`, valor cru em `Authorization` ou `X-API-Key`, como acima.

```bash
python3 testes/resttest_get.py --help
python3 testes/resttest_post.py --help
```

Rodando sem argumentos, ambos mostram exemplos pensados para o seu `.env`.

Consulta rápida:

```bash
python3 testes/resttest_get.py --base-url http://127.0.0.1:8001 --codplantacao PLANTDEMO --auth bearer
python3 testes/resttest_get.py --base-url http://127.0.0.1:8001 --codplantacao PLANTDEMO --auth x-api-key
python3 testes/resttest_get.py --base-url http://127.0.0.1:8001 --codplantacao PLANTDEMO --auth none
```

Último caso força sem credencial para ver 401 quando o servidor exige token.

Post mínimo (datas e hora podem ser geradas pelo script se você omitir):

```bash
python3 testes/resttest_post.py --base-url http://127.0.0.1:8001 \
  --codplantacao PLANTDEMO --codleitura LEIT001 --lat -22.9 --lon -43.17
```

Um JSON grande de exemplo (A = texto de teste, 0 = número):

```json
{
  "codplantacao": "A",
  "codleitura": "A",
  "codsensor": "A",
  "lat": 0,
  "lon": 0,
  "dataleit": "2026-05-07",
  "horaleit": "12:00:00",
  "temp_solo": 0,
  "temp_ar": 0,
  "umid_solo": 0,
  "umid_ar": 0,
  "luz": 0,
  "chuva": 0,
  "umid_folha": 0,
  "scomunicacao": 0,
  "stensao": 0,
  "scorrente": 0,
  "spotencia": 0,
  "ref_rssi_dbm": 0,
  "rec_rssi_dbm": 0,
  "fator_n": 0,
  "distcalc_app": 0,
  "status_blockchain": "PENDENTE",
  "hash_blockchain": "A",
  "tx_hash": "A",
  "criadoem": "2026-05-07T12:00:00"
}
```

Salvando como `leitura.json`:

```bash
python3 testes/resttest_post.py --base-url http://127.0.0.1:8001 --json-file leitura.json
```

## GET `/leituras` (filtros)

Pelo menos um entre: `codplantacao`, `dataleit_inicio`, `dataleit_fim` (formato de data `YYYY-MM-DD`). `limit` (1 a 500, padrão 100) e `offset` são opcionais.

```text
GET http://192.168.1.10:8001/leituras?codplantacao=PLANTDEMO&dataleit_inicio=2026-05-01&dataleit_fim=2026-05-31
```

Se o servidor tiver `API_TOKEN`, inclua header de autorização no cliente.

## POST `/leituras` (corpo JSON)

Obrigatórios: `codplantacao`, `codleitura`, `lat`, `lon`, `dataleit`, `horaleit`. Data no formato `YYYY-MM-DD`; hora `HH:MM` ou `HH:MM:SS`. O restante é opcional; campos omitidos viram sentinela `-9999` no banco, como na definição da tabela. Incluem sensores ambientais e, se quiser, `scomunicacao`, `stensao`, `scorrente`, `spotencia`. `status_blockchain` aceita `PENDENTE`, `ENVIADO` ou `CONFIRMADO` (padrão `PENDENTE`).

Aliases camelCase também entram (`RefRSSIdBm` e outros; veja Swagger).

```bash
curl -X POST "http://127.0.0.1:8001/leituras" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer SEU_TOKEN" \
  -d '{
    "codplantacao": "PLANTDEMO",
    "codleitura": "LEIT001",
    "lat": -22.9068,
    "lon": -43.1729,
    "dataleit": "2026-05-01",
    "horaleit": "14:30:00",
    "temp_solo": 25.5,
    "temp_ar": 28.3,
    "umid_solo": 60.2,
    "umid_ar": 55.1,
    "luz": 800.0,
    "chuva": 0.0,
    "umid_folha": 10.5,
    "scomunicacao": 1.0,
    "stensao": 220.0,
    "scorrente": 0.5,
    "spotencia": 110.0,
    "ref_rssi_dbm": -50.0,
    "rec_rssi_dbm": -55.0,
    "fator_n": 0.0,
    "distcalc_app": 0.0,
    "codsensor": "SENSOR001",
    "status_blockchain": "PENDENTE"
  }'
```

Você também pode usar só `-H "Authorization: SEU_TOKEN"` se preferir igual ao Swagger.

Respostas comuns: 201 com `hash_pk`, 400 de validação, 401 se faltar ou errar token, 409 duplicidade na chave derivada, 500 erro de persistência ou conexão.

## Android em Kotlin com OkHttp

Aponte `baseUrl` para a máquina que roda o Flask. Emulador costuma usar `http://10.0.2.2:8001` para falar com o `localhost` do PC. Aparelho físico usa o IP da LAN (`http://192.168.x.x:8001`), na mesma rede ou com túnel.

Permissão de internet no manifest:

```xml
<uses-permission android:name="android.permission.INTERNET" />
```

HTTP sem TLS em laboratório pode exigir exceção de cleartext; limita isso a build de debug.

No `build.gradle` do módulo:

```kotlin
dependencies {
    implementation("com.squareup.okhttp3:okhttp:4.12.0")
}
```

O Swagger descreve o mesmo contrato de `POST /leituras` que você espelha no JSON abaixo. Exemplo síncrono (trate thread na UI como preferir):

```kotlin
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.json.JSONObject
import java.time.LocalDate
import java.time.LocalTime
import java.time.format.DateTimeFormatter

fun enviarLeitura(
    baseUrl: String,
    codPlantacao: String,
    codLeitura: String,
    lat: Double,
    lon: Double,
    tempSolo: Double?,
    tempAr: Double?,
    scomunicacao: Double? = null,
    stensao: Double? = null,
    scorrente: Double? = null,
    spotencia: Double? = null,
): Result<String> = runCatching {
    val hoje = LocalDate.now().format(DateTimeFormatter.ISO_LOCAL_DATE)
    val agora = LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"))

    val json = JSONObject().apply {
        put("codplantacao", codPlantacao)
        put("codleitura", codLeitura)
        put("lat", lat)
        put("lon", lon)
        put("dataleit", hoje)
        put("horaleit", agora)
        tempSolo?.let { put("temp_solo", it) }
        tempAr?.let { put("temp_ar", it) }
        scomunicacao?.let { put("scomunicacao", it) }
        stensao?.let { put("stensao", it) }
        scorrente?.let { put("scorrente", it) }
        spotencia?.let { put("spotencia", it) }
        put("status_blockchain", "PENDENTE")
    }

    val client = OkHttpClient()
    val body = json.toString().toRequestBody("application/json; charset=utf-8".toMediaType())
    val request = Request.Builder()
        .url("$baseUrl/leituras")
        .post(body)
        .build()

    client.newCall(request).execute().use { response ->
        val texto = response.body?.string().orEmpty()
        if (!response.isSuccessful) {
            error("HTTP ${response.code}: $texto")
        }
        texto
    }
}
```

Se precisar de token: `.header("Authorization", "Bearer $token")` ou o mesmo texto que você configurou no servidor.

`execute()` bloqueia a thread atual; na Activity use coroutine em `Dispatchers.IO` ou `enqueue` do OkHttp. Em produção, HTTPS e validação de certificado são o caminho esperado.

## Docker Compose

Para subir API e Postgres sem instalar Python localmente, há `Dockerfile` e `docker-compose.yml`. Na primeira criação do volume, o Postgres roda `scripts_bd/create_table.sql`. A API sobe na porta publicada como 8001 (ou `API_PORT`).

```bash
docker compose build
docker compose up -d
```

Swagger: `http://localhost:8001/apidocs`. WSDL: `http://localhost:8001/soap?wsdl`. Exemplo rápido: `http://localhost:8001/soap?format=json&codplantacao=PLANTDEMO`.

Variáveis usuais via `.env` na pasta do projeto ou ambiente:

| Variável | Padrão | Significado |
|----------|--------|-------------|
| `POSTGRES_USER` | `bluet` | usuário SQL |
| `POSTGRES_PASSWORD` | `bluet_secret` | senha |
| `POSTGRES_DB` | `bluet` | nome do banco |
| `POSTGRES_PORT` | `5432` | porta exposta no host |
| `API_PORT` | `8001` | porta da API no host |
| `API_TOKEN` | vazio | mesmo papel do `.env` local |
| `SOAP_PUBLIC_URL` | vazio | URL pública do SOAP em deploy |
| `SOAP_NAMESPACE` | vazio | namespace do WSDL |

Dentro da rede do Compose a API monta `DATABASE_URL` automaticamente para o hostname `db`.

`docker compose down` encerra contêineres. `docker compose down -v` apaga também o volume (perde dados do Postgres).

Se o volume já existiu de antes, o `initdb` não roda outra vez. Aí vale conferir se a tabela está completa; se faltar coluna, use o trecho comentado no fim de `scripts_bd/create_table.sql` como guia para um `ALTER` manual pontual via `psql` no serviço `db`.
