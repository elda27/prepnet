from dataclasses import dataclass
from typing import Dict
from prepnet.executor.state_value import StateValue

class StateManager(dict):
    @dataclass
    class State:
        status: StateValue
        origin: object
        columns: str

    def __init__(self, converters: Dict[str, object]):
        super().__init__()
        for key, converter in converters.items():
            self[converter] = self.State(
                status=StateValue.Prepared,
                origin=converter.origin,
                columns=key,
            )

    def set_status(self, converter, status: StateValue):
        self[converter].status = status

    def prepare(self, converter):
        state = self[converter]
        state.status = StateValue.Prepared

    def set_prepare(self):
        for key in self.keys():
            self.prepare(key)

    def is_prepared(self, converter):
        return self[converter].status == StateValue.Prepared

    def queue(self, converter):
        self[converter].status = StateValue.Queued

    def is_queued(self, converter):
        return self[converter].status == StateValue.Queued

    def run(self, converter):
        state = self[converter]
        state.status = StateValue.Running

    def is_running(self, converter):
        return self[converter].status == StateValue.Running

    def finish(self, converter):
        self[converter].status = StateValue.Finished

    def is_finished(self, converter):
        return self[converter].status == StateValue.Running

    def is_states(self, converter, values: StateValue):
        return self[converter].status in values

    def is_all_finished(self):
        return all([self[k].status == StateValue.Finished for k in self.keys()])

    def is_all_queued(self):
        return all([
            self[k].status in (StateValue.Queued, StateValue.Finished) 
            for k in self.keys()
        ]) and any([self[k].status == StateValue.Queued for k in self.keys()])

