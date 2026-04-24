from enum import Enum
from typing import Optional
from uuid import UUID

from pydantic import BaseModel

from backend.lib.notifs.types import NotificationType


class JobType(Enum):
    # Local job types
    LOCAL_ASSET_COMPRESS_UPLOAD = "local_asset_compress_upload"

    # Remote job types
    REMOTE_PHOTOBOOK_GENERATION = "remote_photobook_generation"
    REMOTE_POST_PROCESS_UPLOADED_ASSETS = "remote_post_process_uploaded_assets"
    REMOTE_DELIVER_NOTIFICATION = "remote_deliver_notification"


class JobInputPayload(BaseModel):
    user_id: UUID
    originating_photobook_id: Optional[UUID]


class JobOutputPayload(BaseModel):
    job_id: UUID


#####################################################################################
# Local processor input / output
#####################################################################################


#####################################################################################
# Remote processor input / output
#####################################################################################
class PhotobookGenerationInputPayload(JobInputPayload):
    asset_ids: list[UUID]


class PhotobookGenerationOutputPayload(JobOutputPayload):
    gemini_output_raw_json: Optional[str] = None
    raw_llm_prompt: Optional[str] = None
    selected_photo_file_names: Optional[list[list[str]]] = []


class PostProcessUploadedAssetsInputPayload(JobInputPayload):
    asset_ids: list[UUID]


class PostProcessUploadedAssetsOutputPayload(JobOutputPayload):
    assets_rejected_invalid_mime: list[UUID]
    assets_post_process_failed: list[UUID]
    assets_post_process_succeeded: list[UUID]


class DeliverNotificationInputPayload(JobInputPayload):
    notification_type: NotificationType
    share_channel_id: UUID


class DeliverNotificationOutputPayload(JobOutputPayload):
    pass
