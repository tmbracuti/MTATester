import threading


class PDriver(threading.Thread):
    def __init__(self, props, rbase, test_inputs, keep_timings, fail_out, tid, test_function):
        threading.Thread.__init__(self)
        self.test = test_function
        self._props = props
        self._rbase = rbase
        self._test_inputs = test_inputs
        self._keep_timings = keep_timings
        self._fail_out = fail_out
        self._tid = tid
        self.res_tuple = None
        print(f'worker {self._tid} working')

    def run(self):
        try:
            print(f'{self._tid} executing')
            self.res_tuple = self.test(self._props, self._rbase, self._test_inputs, self._keep_timings, self._fail_out, self._tid)
            print(f'{self._tid} complete')
        except Exception as e:
            print('error executing parallel sync_test: ', str(e))

