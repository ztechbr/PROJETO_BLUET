# Servidor API — Projeto BlueSensores (UTFPR)

API em Python com **Flask**, documentação **Swagger** para o REST e um **Web Service SOAP 1.1** opcional, pensada para **testes de integração** com o **aplicativo Android de sensores** e para clientes legados. A API recebe leituras em JSON via `POST` (REST), grava na tabela PostgreSQL `leituras`, e permite **consultas filtradas** tanto pelo **`GET /leituras`** quanto pela operação SOAP **`listarLeituras`** — os dois usam as mesmas regras de filtro.

**Repositório no GitHub:** use o nome **`Servidor_API_Projeto_BlueSensores_UTFPR`**. Para renomear um repositório já criado: *Settings → General → Repository name*.

> **Escopo:** ambiente de desenvolvimento e testes — não use esta configuração (debug, servidor embutido) em produção sem endurecimento adequado (HTTPS, autenticação, processo WSGI, etc.).

## Pré-requisitos

- Python 3.10+ (recomendado)
- PostgreSQL com a tabela criada conforme `scripts_bd/create_table.sql`
- Se o banco **já existia** antes da inclusão dos campos elétricos, aplique também `scripts_bd/alter_leituras_add_eletricos.sql` uma vez
- Rede acessível entre o celular/emulador e a máquina que roda a API (mesma Wi‑Fi ou túnel)

## Configuração do banco (`.env`)

Na raiz do projeto, copie o modelo e ajuste:

```bash
cp .env.example .env
```

Você pode usar **uma** das formas:

1. **URL completa:** `DATABASE_URL=postgresql://usuario:senha@host:5432/nome_do_banco`
2. **Variáveis separadas:** `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`

Opcional: `PORT` — porta HTTP da API (padrão **8001** se não definido).

## Instalação e execução

```bash
cd Servidor_API_Projeto_BlueSensores_UTFPR
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python app.py
```

Por padrão o servidor sobe em **`http://0.0.0.0:8001`** (acessível na rede local pelo IP da máquina).

## Endpoints

| Método | Caminho | Descrição |
|--------|---------|-----------|
| `GET` | `/health` | Verifica se o serviço está no ar (`{"status":"ok"}`). |
| `GET` | `/leituras` | Lista leituras com filtros (obrigatório pelo menos um filtro). |
| `POST` | `/leituras` | Insere uma leitura (JSON). |
| `POST` | `/soap` | Consulta leituras via **SOAP 1.1** (equivalente ao `GET /leituras`; ver abaixo). |

### Como escolher: REST (Swagger) ou Web Service (SOAP)

| Objetivo | Caminho recomendado |
|----------|----------------------|
| Explorar e testar a API no navegador, ver parâmetros e exemplos | **Swagger** em `/apidocs` |
| Integrar apps modernos, mobile ou scripts com JSON | **REST** (`GET`/`POST` em `/leituras`) |
| Integrar sistemas mais antigos ou ferramentas que só falam SOAP/WSDL | **Web Service** em `/soap` + WSDL |

A **gravação** de novas leituras está disponível **apenas por REST** (`POST /leituras`). A **leitura com filtros** pode ser feita por **REST** ou **SOAP**, com o mesmo significado de filtros.

---

### Documentação REST — Swagger UI

1. Com o servidor em execução, abra no navegador: **`http://<host>:<porta>/apidocs`**  
   (ex.: `http://127.0.0.1:8001/apidocs` ou `http://localhost:8001/apidocs` com Docker).
2. A interface **Swagger UI** lista os endpoints (`/health`, `GET /leituras`, `POST /leituras`), os parâmetros (query, body) e os códigos de resposta descritos no projeto.
3. Para **experimentar** uma rota: expanda a operação → **Try it out** → preencha parâmetros ou o JSON do corpo → **Execute**.
4. A própria UI mostra o **curl** gerado e o corpo da resposta, o que ajuda a repetir a chamada em Postman, no app ou em scripts.

Assim você valida contratos e URLs sem escrever código à mão.

---

### Web Service SOAP — consulta de leituras (compatível com clientes legados)

Para sistemas que não usam REST/JSON, a API expõe um serviço **SOAP 1.1** cuja operação de consulta replica os **mesmos filtros** do `GET /leituras` (pelo menos um entre plantação e intervalo de datas; `limit` e `offset` opcionais).

| Item | Valor |
|------|--------|
| **WSDL (contrato para importar no cliente)** | `http://<host>:<porta>/soap/?wsdl` |
| **Endpoint HTTP** | `POST` em `http://<host>:<porta>/soap` |
| **Corpo** | XML envelope SOAP 1.1 (veja exemplo abaixo) |
| **Operação** | `listarLeituras` |
| **Namespace XML** | `http://utfpr.edu.br/bluesensores/leituras` |
| **Cabeçalho** | `Content-Type: text/xml; charset=utf-8` e, em geral, `SOAPAction: listarLeituras` (confira no WSDL se seu cliente exigir outro formato). |

O elemento de entrada **`filtro`** aceita campos opcionais alinhados ao REST: `codplantacao`, `dataleit_inicio`, `dataleit_fim` (datas `YYYY-MM-DD`), `limit` (1–500, padrão 100), `offset` (padrão 0). Se **nenhum filtro** for informado, o serviço responde com **SOAP Fault**, como no `GET` sem filtros.

Exemplo de envelope (ajuste host, porta e valores):

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

**Sugestão de teste rápido com curl** (envia o XML acima salvo em `request.xml`):

```bash
curl -s -X POST "http://127.0.0.1:8001/soap" \
  -H "Content-Type: text/xml; charset=utf-8" \
  -H "SOAPAction: listarLeituras" \
  --data-binary @request.xml
```

Em **.NET**, **Java** (JAX-WS, CXF) ou **SoapUI** / **Postman**, use **Importar WSDL** a partir da URL `/soap/?wsdl` e gere ou configure o cliente apontando para o mesmo host e porta da API.

> **Nota:** a instalação do projeto pode exigir **Git** para baixar a dependência SOAP (Spyne) no Python 3.12+; o `Dockerfile` já instala o pacote `git` só durante o build da imagem.

---

### GET `/leituras` — filtros (query string)

Informe **pelo menos um** dos filtros abaixo:

- `codplantacao` — código da plantação
- `dataleit_inicio` — data inicial inclusiva (`YYYY-MM-DD`)
- `dataleit_fim` — data final inclusiva (`YYYY-MM-DD`)

Opcionais: `limit` (1–500, padrão 100), `offset` (paginação).

Exemplo:

```text
GET http://192.168.1.10:8001/leituras?codplantacao=PLANTDEMO&dataleit_inicio=2026-05-01&dataleit_fim=2026-05-31
```

### POST `/leituras` — corpo JSON

Campos **obrigatórios:** `codplantacao`, `codleitura`, `lat`, `lon`, `dataleit`, `horaleit`.

- `dataleit`: string `YYYY-MM-DD`
- `horaleit`: string `HH:MM` ou `HH:MM:SS`

Demais campos numéricos são opcionais; se omitidos, a API usa o valor sentinela **-9999** (alinhado aos defaults da tabela). Entre eles estão os sensores ambientais habituais e, opcionalmente, **comunicação / grandezas elétricas:** `scomunicacao`, `stensao`, `scorrente`, `spotencia`. `status_blockchain` pode ser `PENDENTE`, `ENVIADO` ou `CONFIRMADO` (padrão `PENDENTE`).

Exemplo com `curl`:

```bash
curl -X POST "http://127.0.0.1:8001/leituras" \
  -H "Content-Type: application/json" \
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
    "status_blockchain": "PENDENTE"
  }'
```

Respostas comuns: **201** (criado, retorna `hash_pk`), **400** (validação), **409** (leitura duplicada pela chave gerada), **500** (erro de banco ou conexão).

---

## Android: enviar leitura com `POST` em Kotlin

No app de sensores, use a URL base apontando para o computador que executa o Flask:

- **Emulador Android:** para o `localhost` da máquina host, use **`http://10.0.2.2:<PORTA>`** (por exemplo `8001`).
- **Dispositivo físico:** use o **IP da máquina na LAN** (ex.: `http://192.168.1.10:8001`). O celular e o PC devem estar na mesma rede (ou use um túnel tipo ngrok).

No `AndroidManifest.xml`, declare permissão de internet:

```xml
<uses-permission android:name="android.permission.INTERNET" />
```

Se usar **HTTP** (não HTTPS) em testes, pode ser necessário permitir cleartext ou configurar *Network Security Config* para o domínio/IP de desenvolvimento (somente em debug).

### Exemplo com OkHttp

No **`/apidocs`** (Swagger), o corpo de **`POST /leituras`** já lista os campos opcionais numéricos, inclusive **`scomunicacao`**, **`stensao`**, **`scorrente`** e **`spotencia`** — o mesmo contrato usado abaixo.

Adicione no `build.gradle` do módulo (versões podem ser atualizadas):

```kotlin
dependencies {
    implementation("com.squareup.okhttp3:okhttp:4.12.0")
}
```

Exemplo de função (execute em `Dispatchers.IO` dentro de uma coroutine, ou use `enqueue` do OkHttp para não bloquear a UI):

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
    baseUrl: String, // ex: "http://10.0.2.2:8001" ou "http://192.168.1.10:8001"
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

Pontos importantes:

1. **`Content-Type: application/json`** — o exemplo usa `application/json; charset=utf-8` no `RequestBody`, compatível com a API.
2. **Datas e hora** — a API espera `dataleit` como `YYYY-MM-DD` e `horaleit` como `HH:MM` ou `HH:MM:SS`; o exemplo usa data/hora atuais do dispositivo.
3. **Thread** — `execute()` bloqueia a thread atual; em Activity chame de uma coroutine com `withContext(Dispatchers.IO) { enviarLeitura(...) }` ou use `enqueue` do OkHttp.
4. **Produção** — troque HTTP por HTTPS, valide certificados e adicione autenticação se a API for exposta na internet.

Com isso você integra o app Android de sensores aos testes desta API Flask de forma alinhada ao modelo da tabela `leituras`.

---

## Rodando com Docker (alternativa à linha de comando)

Além de instalar Python na máquina e executar `python app.py`, você pode subir **API + PostgreSQL** com **Docker Compose**. O repositório inclui um `Dockerfile` (imagem da API) e um `docker-compose.yml` que:

- sobe o PostgreSQL e, na **primeira** inicialização do volume, aplica `scripts_bd/create_table.sql` para criar a tabela `leituras`;
- constrói e inicia a API Flask na porta **8001** dentro da rede do Compose, apontando `DATABASE_URL` para o serviço `db`.

### Pré-requisitos

- [Docker](https://docs.docker.com/get-docker/) e [Docker Compose](https://docs.docker.com/compose/) (plugin `docker compose`).

### Comandos

Na raiz do projeto:

```bash
docker compose build
docker compose up -d
```

- Documentação Swagger: `http://localhost:8001/apidocs` (ou a porta definida em `API_PORT`).
- O Postgres fica exposto no host na porta **5432** por padrão (`POSTGRES_PORT`).

### Variáveis opcionais

Você pode definir no ambiente ou num arquivo `.env` **na pasta do projeto** (usado pelo Compose para interpolação):

| Variável | Padrão | Uso |
|----------|--------|-----|
| `POSTGRES_USER` | `bluet` | usuário do banco |
| `POSTGRES_PASSWORD` | `bluet_secret` | senha |
| `POSTGRES_DB` | `bluet` | nome do banco |
| `POSTGRES_PORT` | `5432` | porta do Postgres no host |
| `API_PORT` | `8001` | porta da API no host |

A API dentro do contêiner usa `DATABASE_URL` gerado automaticamente a partir desses valores e do hostname `db`.

### Observações

- Para **parar** e remover contêineres: `docker compose down`. Para apagar também o volume do Postgres (apaga os dados): `docker compose down -v`.
- Se o volume do banco **já existir** de uma execução anterior, o script de `initdb` **não roda de novo**. Nesse caso, garanta que a tabela exista (ou recrie o volume, ciente de que os dados serão perdidos). Se a tabela foi criada sem as colunas `scomunicacao`, `stensao`, `scorrente` e `spotencia`, execute `scripts_bd/alter_leituras_add_eletricos.sql` no Postgres (por exemplo com `psql` apontando para o serviço `db`).

Assim você pode testar o mesmo fluxo do app Android e os endpoints REST usando apenas Docker, sem precisar configurar Python e Postgres diretamente no sistema operacional.
