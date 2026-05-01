# API Leituras (Flask)

API REST em Python com **Flask** e documentação **Swagger**, pensada para **testes de integração** com o **aplicativo Android de sensores**. Ela recebe leituras em JSON via `POST`, grava na tabela PostgreSQL `leituras`, e permite consultas filtradas via `GET`.

> **Escopo:** ambiente de desenvolvimento e testes — não use esta configuração (debug, servidor embutido) em produção sem endurecimento adequado (HTTPS, autenticação, processo WSGI, etc.).

## Pré-requisitos

- Python 3.10+ (recomendado)
- PostgreSQL com a tabela criada conforme `scripts_bd/create_table.sql`
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
cd PROJETO_BLUET
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

Documentação interativa: **`http://<host>:<porta>/apidocs`**

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

Demais campos numéricos são opcionais; se omitidos, a API usa o valor sentinela **-9999** (alinhado aos defaults da tabela). `status_blockchain` pode ser `PENDENTE`, `ENVIADO` ou `CONFIRMADO` (padrão `PENDENTE`).

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
    // ... outros sensores conforme necessário
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
