from __future__ import annotations
from app import app
from abc import ABC, abstractmethod
from app.event_hub.sender import send_data, loop
from app.constants import Config
from app.event_hub.sender import send_data, loop

class Context():

    def __init__(self) -> None:
        pass
    
    def __init__(self, logging: Logging) -> None:
        self._logging = logging

    @property
    def logging(self) -> Logging:
        return self._logging

    @logging.setter
    def logging(self, logging: Logging) -> None:

        self._logging = logging

    def do_logging(self, data) -> None:
        self._logging.log(data)


class Logging(ABC):

    @abstractmethod
    def log(self, data: str):
        pass


class ConsoleLogging(Logging):
    
    def __init__(self):
        app.logger.info("EventHubLogging")
    
    def log(self, data: str) -> None:
        app.logger.info(data)


class EventHubLogging(Logging):
    
    def __init__(self):
        app.logger.info("EventHubLogging")
        
    def log(self, data: str) -> None:
        loop.run_until_complete(send_data(data))

context = Context

if (Config.CONSOLE_LOGGING):
    context = Context(ConsoleLogging())
else:
    context = Context(EventHubLogging())
# context.do_logging()
print(Context.logging)
