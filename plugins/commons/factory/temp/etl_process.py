from abc import abstractmethod


class ETLProcess:
    @abstractmethod
    def extract(self):
        raise NotImplementedError('this is abstract method')

    @abstractmethod
    def transform(self):
        raise NotImplementedError('this is abstract method')

    @abstractmethod
    def load(self):
        raise NotImplementedError('this is abstract method')



