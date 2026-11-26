"""FastDepends context annotations for Iggy."""

from typing import Annotated

from apache_iggy import IggyClient as IggyClientDriver

from faststream._internal.context import Context
from faststream.annotations import ContextRepo, Logger
from faststream.params import NoCast

from .broker.broker import IggyBroker as IggyBrokerDriver
from .message import IggyMessage as IggyMessageDriver
from .publisher.producer import IggyProducer as IggyProducerDriver

__all__ = (
    "ContextRepo",
    "IggyBroker",
    "IggyClient",
    "IggyMessage",
    "IggyProducer",
    "Logger",
    "NoCast",
)

IggyBroker = Annotated[IggyBrokerDriver, Context("broker")]
IggyClient = Annotated[IggyClientDriver, Context("broker._connection")]
IggyMessage = Annotated[IggyMessageDriver, Context("message")]
IggyProducer = Annotated[IggyProducerDriver, Context("broker._producer")]
