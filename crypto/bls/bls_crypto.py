from abc import ABCMeta, abstractmethod
from collections import namedtuple
from typing import Sequence

GroupParams = namedtuple('GroupParams',
                         'group_name, g')


class BlsGroupParamsLoader(metaclass=ABCMeta):
    @abstractmethod
    def load_group_params(self) -> GroupParams:
        pass


class BlsCryptoSigner(metaclass=ABCMeta):
    def __init__(self, sk: str, pk: str, params: GroupParams):
        assert sk
        assert pk
        self._sk = sk
        self.pk = pk
        self._group_params = params

    @staticmethod
    @abstractmethod
    def generate_keys(params: GroupParams, seed=None) -> (str, str):
        pass

    @abstractmethod
    def sign(self, message: bytes) -> str:
        pass


class BlsCryptoVerifier(metaclass=ABCMeta):
    @abstractmethod
    def create_multi_sig(self, signatures: Sequence[str]) -> str:
        pass

    @abstractmethod
    def verify_sig(self, signature: str, message: bytes, pk: str) -> bool:
        pass

    @abstractmethod
    def verify_multi_sig(self, signature: str, message: bytes, pks: Sequence[str]) -> bool:
        pass
