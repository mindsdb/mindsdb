from mindsdb.integrations.libs.const import HANDLER_TYPE
from mindsdb.utilities.log import get_log
from youtube_transcript_api import YouTubeTranscriptApi

logger = get_log()

from .__about__ import __version__ as version, __description__ as description

class YoutubeHandler:
    def get_transcript(self, video_id):
        try:
            transcript = YouTubeTranscriptApi.get_transcript(video_id)
            return transcript
        except Exception as e:
            logger.error(f"Error retrieving transcript for video {video_id}: {e}")
            return []

try:
    from .youtube_handler import YoutubeHandler as Handler
    logger.info("No error Importing Youtube API Handler")
    import_error = None
except Exception as e:
    Handler = None
    import_error = e
    logger.info(f"Error Importing Youtube API Handler: {e}!")

title = "YouTube"
name = "youtube"
type = HANDLER_TYPE.DATA
icon_path = "icon.png"

__all__ = [
    "Handler",
    "version",
    "name",
    "type",
    "title",
    "description",
    "import_error",
    "icon_path",
]
