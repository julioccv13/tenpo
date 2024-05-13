from dataclasses import dataclass, asdict, field
from typing import Dict, Optional
import json
from datetime import datetime

from core.models.parseable import Parseable
from core.common import get_deep_value_or_null


@dataclass
class DataUniqueRequestModel(Parseable):
    event_name: Optional[str]
    from_: int
    to: int
    unique: Optional[bool] = False
    groups: Optional[Dict[str, Dict[str, str]]] = field(
        default_factory=lambda: {"foo": {"trend_type": "daily"}})

    @classmethod
    def parse(cls, event, from_, to) -> str:
        _instance = cls(event_name=event['name'],
                        from_=from_,
                        to=to)
        _instance_dict = asdict(_instance)
        _instance_dict["from"] = _instance_dict.pop("from_")
        return json.dumps(_instance_dict)


@dataclass
class DataRequestModel(Parseable):
    event_name: Optional[str]
    from_: int
    to: int

    @classmethod
    def parse(cls, event, from_, to):
        _instance = cls(event_name=event['name'],
                        from_=from_,
                        to=to)
        _instance_dict = asdict(_instance)
        _instance_dict["from"] = _instance_dict.pop("from_")
        return json.dumps(_instance_dict)


@dataclass
class DataTrendResponseModel(Parseable):
    event: Optional[str]
    date: Optional[str]
    year: Optional[int]
    month: Optional[int]
    day: Optional[int]
    count: Optional[int]

    @classmethod
    def parse(cls, event, trend_data_list):
        data_list = []
        foo = trend_data_list.get('foo', {})
        for key, value in foo.items():
            _instance = cls(
                event=str(event['name']),
                date=key,
                year=int(str(key)[0:4]),
                month=int(str(key)[4:6]),
                day=int(str(key)[6:8]),
                count=int(value)
            )
            data_item = asdict(_instance)
            data_list.append(data_item)
        return data_list


@dataclass
class AppInstalledModel(Parseable):
    event: str
    objectId: str
    plataform: str
    session_source: str
    date: int
    year: int
    month: int
    day: int
    fecha_hora: str
    name: str
    email: str
    identity: str
    os_version: str
    app_version: str
    make: str
    model: str

    @classmethod
    def parse(cls, data):
        ts = str(data['ts'])
        return dict(
            event=data['event'],
            objectId=get_deep_value_or_null(data, ["profile", "objectId"]),
            platform=get_deep_value_or_null(data, ["profile", "platform"]),
            session_source=get_deep_value_or_null(
                data, ["session_props", "session_source"]),
            date=int(ts[0:8]),
            year=int(ts[0:4]),
            month=int(ts[4:6]),
            day=int(ts[6:8]),
            fecha_hora=str(datetime.strptime(ts, "%Y%m%d%H%M%S")),
            name=get_deep_value_or_null(data, ["profile", "name"]),
            email=get_deep_value_or_null(data, ["profile", "email"]),
            identity=get_deep_value_or_null(data, ["profile", "identity"]),
            os_version=get_deep_value_or_null(data, ["profile", "os_version"]),
            app_version=get_deep_value_or_null(
                data, ["profile", "app_version"]),
            make=get_deep_value_or_null(data, ["profile", "make"]),
            model=get_deep_value_or_null(data, ["profile", "model"])
        )


@dataclass
class DataOnfidoModel(Parseable):
    event: str
    date: int
    year: int
    month: int
    day: int
    fecha_hora: str
    email: str
    phone: str
    attempt: str
    type_: str
    result: str
    breakdown: str
    CT_Source: str
    identity: str
    rfmp_segment_ult60d: str
    nomolestar: str
    userpersona: str
    betainvitaygana: str
    dob: str
    saldo: str
    trx_top_rubro: str
    rut: str
    top_rubro: str
    unique_rubro: str
    os_version: str
    app_version: str
    make: str
    model: str
    platform: str
    objectId: str

    @classmethod
    def parse(cls, data):
        ts = str(data['ts'])
        return dict(
            event=data['event'],
            date=int(ts[0:8]),
            year=int(ts[0:4]),
            month=int(ts[4:6]),
            day=int(ts[6:8]),
            fecha_hora=str(datetime.strptime(ts, "%Y%m%d%H%M%S")),
            email=get_deep_value_or_null(data, ["profile", "email"]),
            phone=get_deep_value_or_null(data, ["profile", "phone"]),
            attemp=get_deep_value_or_null(data, ["event_props", "attemp"]),
            type=get_deep_value_or_null(data, ["event_props", "type"]),
            result=get_deep_value_or_null(data, ["event_props", "result"]),
            breakdown=get_deep_value_or_null(
                data, ["event_props", "breakdown"]),
            CT_Source=get_deep_value_or_null(
                data, ["event_props", "CT_Source"]),
            identity=get_deep_value_or_null(data, ["profile", "identity"]),
            rfmp_segment_ult60d=get_deep_value_or_null(
                data, ["profile", "profileData", "rfmp_segment_ult60d"]),
            nomolestar=get_deep_value_or_null(
                data, ["profile", "profileData", "nomolestar"]),
            userpersona=get_deep_value_or_null(
                data, ["profile", "profileData", "userpersona"]),
            betainvitaygana=get_deep_value_or_null(
                data, ["profile", "profileData", "betainvitaygana"]),
            dob=get_deep_value_or_null(
                data, ["profile", "profileData", "dob"]),
            saldo=get_deep_value_or_null(
                data, ["profile", "profileData", "saldo"]),
            trx_top_rubro=get_deep_value_or_null(
                data, ["profile", "profileData", "trx_top_rubro"]),
            rut=get_deep_value_or_null(
                data, ["profile", "profileData", "rut"]),
            top_rubro=get_deep_value_or_null(
                data, ["profile", "profileData", "top_rubro"]),
            unique_rubro=get_deep_value_or_null(
                data, ["profile", "profileData", "unique_rubro"]),
            os_version=get_deep_value_or_null(
                data, ["profile", "profileData", "os_version"]),
            app_version=get_deep_value_or_null(
                data, ["profile", "app_version"]),
            make=get_deep_value_or_null(data, ["profile", "make"]),
            model=get_deep_value_or_null(data, ["profile", "model"]),
            platform=get_deep_value_or_null(data, ["profile", "platform"]),
            objectId=get_deep_value_or_null(data, ["profile", "objectId"]),
        )


@dataclass
class EventModel(Parseable):
    event: str
    objectId: str
    push_token: str
    plataform: str
    session_source: str
    date: int
    year: int
    month: int
    day: int
    fecha_hora: str
    ct_session_id: str
    ct_carrier: str
    ct_source: str
    ct_app_v: str
    ct_sdk_v: str
    ct_os_v: str
    name: str
    phone: str
    email: str
    identity: str
    badKs: str
    session_referrer: str
    utm_source: str
    utm_medium: str
    utm_campaign: str
    nombre: str
    correo: str
    wzrk_pivot: str
    campaign_type: str
    wzrk_id: str
    wzrk_rnv: str
    wzrk_dt: str
    wzrk_cts: str
    wzrk_acct_id: str
    wzrk_c2a: str
    wzrk_dl: str
    userId: str
    session_id: str
    session_lenght: str
    tenpoContacts: str
    totalContacts: str
    tenpoContactsPercentage: str
    campaign_id: str
    nombre_comercio: str
    monto_pesos: str
    monto_faltante_pesos: str
    product: str
    description: str
    code: str
    error: str
    value: str

    @classmethod
    def parse(cls, data):
        ts = str(data['ts'])
        return dict(
            event=data['event'],
            objectId=get_deep_value_or_null(data, ["profile", "objectId"]),
            push_token=get_deep_value_or_null(data, ["profile", "push_token"]),
            platform=get_deep_value_or_null(data, ["profile", "platform"]),
            session_source=get_deep_value_or_null(
                data, ["session_props", "session_source"]),
            date=int(ts[0:8]),
            year=int(ts[0:4]),
            month=int(ts[4:6]),
            day=int(ts[6:8]),
            fecha_hora=str(datetime.strptime(ts, "%Y%m%d%H%M%S")),
            ct_session_id=get_deep_value_or_null(
                data, ["event_props", "CT Session Id"]),
            ct_carrier=get_deep_value_or_null(
                data, ["event_props", "CT Network Carrier"]),
            ct_source=get_deep_value_or_null(
                data, ["event_props", "CT Source"]),
            ct_app_v=get_deep_value_or_null(
                data, ["event_props", "CT App Version"]),
            ct_sdk_v=get_deep_value_or_null(
                data, ["event_props", "CT SDK Version"]),
            ct_os_v=get_deep_value_or_null(
                data, ["event_props", "CT OS Version"]),
            name=get_deep_value_or_null(data, ["profile", "name"]),
            email=get_deep_value_or_null(data, ["profile", "email"]),
            phone=get_deep_value_or_null(data, ["profile", "phone"]),
            identity=get_deep_value_or_null(data, ["profile", "identity"]),
            badKs=get_deep_value_or_null(
                data, ["profile", "badKs"], default=[None])[0],
            session_referrer=get_deep_value_or_null(
                data, ["session_props", "session_referrer"]),
            utm_source=get_deep_value_or_null(
                data, ["session_props", "utm_source"]),
            utm_medium=get_deep_value_or_null(
                data, ["session_props", "utm_medium"]),
            utm_campaign=get_deep_value_or_null(
                data, ["session_props", "utm_campaign"]),
            session_id=get_deep_value_or_null(
                data, ["event_props", "Session Id"]),
            session_lenght=get_deep_value_or_null(
                data, ["event_props", "Session Length"]),
            nombre=get_deep_value_or_null(data, ["event_props", "nombre"]),
            correo=get_deep_value_or_null(data, ["event_props", "correo"]),
            wzrk_pivot=get_deep_value_or_null(
                data, ["event_props", "wzrk_pivot"]),
            campaign_type=get_deep_value_or_null(
                data, ["event_props", "Campaign type"]),
            wzrk_id=get_deep_value_or_null(data, ["event_props", "wzrk_id"]),
            wzrk_rnv=get_deep_value_or_null(data, ["event_props", "wzrk_rnv"]),
            wzrk_dt=get_deep_value_or_null(data, ["event_props", "wzrk_dt"]),
            wzrk_cts=get_deep_value_or_null(data, ["event_props", "wzrk_cts"]),
            wzrk_acct_id=get_deep_value_or_null(
                data, ["event_props", "wzrk_acct_id"]),
            wzrk_c2a=get_deep_value_or_null(data, ["event_props", "wzrk_c2a"]),
            wzrk_dl=get_deep_value_or_null(data, ["event_props", "wzrk_dl"]),
            userId=get_deep_value_or_null(data, ["event_props", "userId"]),
            tenpoContacts=get_deep_value_or_null(
                data, ["event_props", "tenpoContacts"]),
            totalContacts=get_deep_value_or_null(
                data, ["event_props", "totalContacts"]),
            tenpoContactsPercentage=get_deep_value_or_null(
                data, ["event_pro", "tenpoContactsPercentage"]),
            campaign_id=get_deep_value_or_null(
                data, ["event_props", "Campaign id"]),
            nombre_comercio=get_deep_value_or_null(
                data, ["event_props", "nombre_comercio"]),
            monto_pesos=get_deep_value_or_null(
                data, ["event_props", "monto_pesos"]),
            monto_faltante_pesos=get_deep_value_or_null(
                data, ["event_props", "monto_faltante_pesos"]),
            product=get_deep_value_or_null(data, ["event_props", "product"]),
            description=get_deep_value_or_null(
                data, ["event_props", "description"]),
            code=get_deep_value_or_null(data, ["event_props", "code"]),
            error=get_deep_value_or_null(data, ["event_props", "error"]),
            value=get_deep_value_or_null(data, ["event_props", "value"])
        )
