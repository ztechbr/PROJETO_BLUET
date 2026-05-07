"""
Webservice SOAP 1.1 — consulta de leituras com os mesmos filtros do GET /leituras.
WSDL: GET {base}/soap/?wsdl

- SOAP_NAMESPACE: target namespace explícito no WSDL (xs:schema targetNamespace, prefixo tns).
- SOAP_PUBLIC_URL: URL do endpoint SOAP; fixa <soap:address location="..."/>.
  Se SOAP_NAMESPACE não estiver definido mas SOAP_PUBLIC_URL estiver, o namespace passa a ser
  ``{scheme}://{host}/leituras`` (mesmo host do endpoint público), para alinhar ao deploy.
"""
import os
from urllib.parse import urlparse

from dotenv import load_dotenv
from spyne import Application, rpc, ServiceBase
from spyne.error import Fault
from spyne.model.complex import Array, ComplexModel
from spyne.model.primitive import Double, Integer, Unicode
from spyne.protocol.soap import Soap11
from spyne.server.wsgi import WsgiApplication

from leituras_query import ConsultaLeiturasError, consulta_leituras_desde_strings

load_dotenv()

_DEFAULT_TNS = "http://utfpr.edu.br/bluesensores/leituras"


def _resolve_target_namespace():
    explicit = (os.getenv("SOAP_NAMESPACE") or "").strip().rstrip("/")
    if explicit:
        return explicit
    public = (os.getenv("SOAP_PUBLIC_URL") or "").strip().rstrip("/")
    if public:
        parts = urlparse(public)
        if parts.scheme and parts.netloc:
            return f"{parts.scheme}://{parts.netloc}/leituras"
    return _DEFAULT_TNS


TNS = _resolve_target_namespace()


class FiltroListagemLeituras(ComplexModel):
    __namespace__ = TNS
    codplantacao = Unicode(min_occurs=0, max_len=200)
    dataleit_inicio = Unicode(min_occurs=0, max_len=32)
    dataleit_fim = Unicode(min_occurs=0, max_len=32)
    limit = Integer(min_occurs=0)
    offset = Integer(min_occurs=0)


class LeituraItem(ComplexModel):
    __namespace__ = TNS
    codplantacao = Unicode
    codleitura = Unicode
    codsensor = Unicode(nillable=True)
    lat = Double
    lon = Double
    dataleit = Unicode
    horaleit = Unicode
    temp_solo = Double(nillable=True)
    temp_ar = Double(nillable=True)
    umid_solo = Double(nillable=True)
    umid_ar = Double(nillable=True)
    luz = Double(nillable=True)
    chuva = Double(nillable=True)
    umid_folha = Double(nillable=True)
    scomunicacao = Double(nillable=True)
    stensao = Double(nillable=True)
    scorrente = Double(nillable=True)
    spotencia = Double(nillable=True)
    ref_rssi_dbm = Double(nillable=True)
    rec_rssi_dbm = Double(nillable=True)
    fator_n = Double(nillable=True)
    distcalc_app = Double(nillable=True)
    hash_pk = Unicode
    status_blockchain = Unicode(nillable=True)
    hash_blockchain = Unicode(nillable=True)
    tx_hash = Unicode(nillable=True)
    criadoem = Unicode(nillable=True)


class RespostaListagemLeituras(ComplexModel):
    __namespace__ = TNS
    total = Integer
    limit = Integer
    offset = Integer
    items = Array(LeituraItem)


def _item_from_dict(d):
    def u(k):
        v = d.get(k)
        if v is None:
            return None
        return str(v)

    def f(k):
        v = d.get(k)
        if v is None:
            return None
        return float(v)

    return LeituraItem(
        codplantacao=u("codplantacao"),
        codleitura=u("codleitura"),
        codsensor=u("codsensor"),
        lat=f("lat"),
        lon=f("lon"),
        dataleit=u("dataleit"),
        horaleit=u("horaleit"),
        temp_solo=f("temp_solo"),
        temp_ar=f("temp_ar"),
        umid_solo=f("umid_solo"),
        umid_ar=f("umid_ar"),
        luz=f("luz"),
        chuva=f("chuva"),
        umid_folha=f("umid_folha"),
        scomunicacao=f("scomunicacao"),
        stensao=f("stensao"),
        scorrente=f("scorrente"),
        spotencia=f("spotencia"),
        ref_rssi_dbm=f("ref_rssi_dbm"),
        rec_rssi_dbm=f("rec_rssi_dbm"),
        fator_n=f("fator_n"),
        distcalc_app=f("distcalc_app"),
        hash_pk=u("hash_pk"),
        status_blockchain=u("status_blockchain"),
        hash_blockchain=u("hash_blockchain"),
        tx_hash=u("tx_hash"),
        criadoem=u("criadoem"),
    )


class LeiturasSoapService(ServiceBase):
    @rpc(FiltroListagemLeituras, _returns=RespostaListagemLeituras)
    def listarLeituras(ctx, filtro):
        if filtro is None:
            filtro = FiltroListagemLeituras()
        lim = filtro.limit if filtro.limit is not None else 100
        off = filtro.offset if filtro.offset is not None else 0
        cod = filtro.codplantacao
        d_ini = filtro.dataleit_inicio
        d_fim = filtro.dataleit_fim
        try:
            data = consulta_leituras_desde_strings(
                codplantacao_raw=cod,
                dataleit_inicio_raw=d_ini,
                dataleit_fim_raw=d_fim,
                limit=lim,
                offset=off,
            )
        except ConsultaLeiturasError as e:
            code = "Client" if e.http_status < 500 else "Server"
            raise Fault(faultcode=code, faultstring=e.message) from e

        items = [_item_from_dict(row) for row in data["items"]]
        return RespostaListagemLeituras(
            total=data["total"],
            limit=data["limit"],
            offset=data["offset"],
            items=items,
        )


soap_application = Application(
    [LeiturasSoapService],
    TNS,
    name="LeiturasService",
    in_protocol=Soap11(),
    out_protocol=Soap11(),
)

soap_wsgi_app = WsgiApplication(soap_application)

_public = (os.getenv("SOAP_PUBLIC_URL") or "").strip().rstrip("/")
if _public and soap_wsgi_app.doc.wsdl11 is not None:
    soap_wsgi_app.doc.wsdl11.build_interface_document(_public)
    soap_wsgi_app._wsdl = soap_wsgi_app.doc.wsdl11.get_interface_document()
