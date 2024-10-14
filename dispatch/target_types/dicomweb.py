from pathlib import Path
from requests.exceptions import HTTPError

import pydicom
from common.types import DicomWebTarget, TaskDispatch, Task, GCPDicomWebTarget
from .base import TargetHandler
from .registry import handler_for

from dicomweb_client.api import DICOMwebClient
from dicomweb_client.session_utils import create_session_from_user_pass
from dicomweb_client.ext.gcp.session_utils import create_session_from_gcp_credentials
from dicomweb_client.ext.gcp.uri import GoogleCloudHealthcareURL

import common.config as config

logger = config.get_logger()


@handler_for(DicomWebTarget)
class DicomWebTargetHandler(TargetHandler[DicomWebTarget]):
    view_template = "targets/dicomweb.html"
    edit_template = "targets/dicomweb-edit.html"
    # test_template = "targets/dicomweb-test.html"
    icon = "fa-share-alt"
    display_name = "DICOMweb"

    def create_client(self, target: DicomWebTarget):
        session = None
        headers = None
        if target.http_user and target.http_password:
            session = create_session_from_user_pass(username=target.http_user, password=target.http_password)
        elif target.access_token:
            headers = {"Authorization": "Bearer {}".format(target.access_token)}

        client = DICOMwebClient(
            url=target.url,
            qido_url_prefix=target.qido_url_prefix,
            wado_url_prefix=target.wado_url_prefix,
            stow_url_prefix=target.stow_url_prefix,
            session=session,
            headers=headers,
        )
        return client
    
    def send_to_target(
        self, task_id: str, target: DicomWebTarget, dispatch_info: TaskDispatch, source_folder: Path, task: Task
    ) -> str:
        client = self.create_client(target)
        datasets = [pydicom.dcmread(str(k)) for k in source_folder.glob("**/*.dcm")]
        response = client.store_instances(datasets)
        if len(response.ReferencedSOPSequence) != len(datasets):
            raise Exception("Did not store all datasets", response)

        return ""

    def from_form(self, form: dict, factory, current_target) -> DicomWebTarget:
        url = form["url"]

        for x in [
            "qido_url_prefix",
            "wado_url_prefix",
            "stow_url_prefix",
            "http_user",
            "http_password",
            "access_token",
        ]:
            if x in form and not form[x]:
                form[x] = None

        return DicomWebTarget(**form)

    async def test_connection(self, target: DicomWebTarget, target_name: str):
        client = self.create_client(target)

        results = {}
        try:
            result = client._http_get(target.url)
            results["authentication"] = True
        except HTTPError as e:
            if e.errno == 401:
                results["authentication"] = False
            else:
                results["authentication"] = True

        try:
            client.search_for_studies(limit=1)
            results["QIDO_query"] = True
        except HTTPError as e:
            results["QIDO_query"] = False

        return results


@handler_for(GCPDicomWebTarget)
class GCPDicomWebTargetHandler(TargetHandler[GCPDicomWebTarget]):
    view_template = "targets/gcpdicomweb.html"
    edit_template = "targets/gcpdicomweb-edit.html"
    # test_template = "targets/dicomweb-test.html"
    icon = "fa-share-alt"
    display_name = "GCP DICOMweb"

    def create_client(self, target: GCPDicomWebTarget):
        session = create_session_from_gcp_credentials()
        print(target, "TARGET GCP DICOMWEB")
        project_id=target.project_id
        location=target.location
        dataset_id=target.dataset_id
        dicom_store_id=target.dicom_store_id
        url = GoogleCloudHealthcareURL(project_id=project_id,
                                       location=location,
                                       dataset_id=dataset_id,
                                       dicom_store_id=dicom_store_id)
        client = DICOMwebClient(
            url=str(url),
            session=session
        )
        return client

    def send_to_target(
        self, task_id: str, target: GCPDicomWebTarget, dispatch_info: TaskDispatch, source_folder: Path, task: Task
    ) -> str:
        client = self.create_client(target)
        datasets = [pydicom.dcmread(str(k)) for k in source_folder.glob("**/*.dcm")]
        response = client.store_instances(datasets)
        if len(response.ReferencedSOPSequence) != len(datasets):
            raise Exception("Did not store all datasets", response)

        return ""

    def from_form(self, form: dict, factory, current_target) -> GCPDicomWebTarget:
        print(form, "form GCP DICOMWEB")
        for x in [
            "project_id",
            "location_id",
            "dataset_id",
            "dicom_store_id",
        ]:
            if x in form and not form[x]:
                form[x] = None

        return GCPDicomWebTarget(**form)

    async def test_connection(self, target: GCPDicomWebTarget, target_name: str):
        client = self.create_client(target)

        results = {}
        try:
            result = client._http_get(target.url)
            results["authentication"] = True
        except HTTPError as e:
            if e.errno == 401:
                results["authentication"] = False
            else:
                results["authentication"] = True

        try:
            client.search_for_studies(limit=1)
            results["QIDO_query"] = True
        except HTTPError as e:
            results["QIDO_query"] = False

        return results
