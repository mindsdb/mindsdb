import time
from mindsdb.utilities import log

from mindsdb.utilities.log import initialize_log

from mindsdb.utilities.config import Config
from mindsdb.interfaces.storage import db
from mindsdb.interfaces.chatbot.chatbot_thread import ChatBotThread


class ChatBotMonitor:
    """Starts and stops chatbots based on their configuration in the database."""

    _MONITOR_INTERVAL_SECONDS = 1

    def __init__(self):
        self._active_bots = {}

    def start(self):
        """Starts monitoring chatbots periodically based on database configuration."""
        config = Config()
        db.init()
        initialize_log(config, 'jobs', wrap_print=True)
        self.config = config

        while True:
            try:
                self.check_bots()

            except (SystemExit, KeyboardInterrupt):
                self.stop_all_bots()
                raise
            except Exception as e:
                log.logger.error(e)

            db.session.rollback()  # disable cache
            time.sleep(ChatBotMonitor._MONITOR_INTERVAL_SECONDS)

    def stop_all_bots(self):
        """Stops all active bots."""
        active_bots = list(self._active_bots.keys())
        for bot_id in active_bots:
            self.stop_bot(bot_id)

    def check_bots(self):
        """Periodic check to see if new chatbots need to be started, or if existing chatbots need to be stopped."""
        allowed_bots = set()

        for bot in db.ChatBots.query\
                .filter(db.ChatBots.is_running == True)\
                .all():
            bot_id = bot.id
            allowed_bots.add(bot_id)

            if bot_id not in self._active_bots:
                # Start new bot if configured to run.
                thread = ChatBotThread(bot)

                thread.start()
                self._active_bots[bot_id] = thread

        # Check old bots to stop
        active_bots = list(self._active_bots.keys())
        for bot_id in active_bots:
            if bot_id not in allowed_bots:
                self.stop_bot(bot_id)

    def stop_bot(self, bot_id: int):
        """
        Stops a bot by id.

        Parameters:
            bot_id (int): ID of the bot to stop.
        """
        self._active_bots[bot_id].stop()
        del self._active_bots[bot_id]


def start(verbose=False):
    """Creates a new ChatBotMonitor based on MindsDB config and starts monitoring chatbots."""
    is_cloud = Config().get('cloud', False)
    if is_cloud is True:
        # Chatbots are disabled on cloud
        # TODO(tmichaeldb): Remove for public Chatbots release.
        return

    monitor = ChatBotMonitor()
    monitor.start()


if __name__ == '__main__':
    start()
