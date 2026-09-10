from airflow.sdk.bases.operator import BaseOperator

class MyBasicMathOperator(BaseOperator):
    """
    Example Operator that does basic arithmetic.
    :param first_number: first number to put into an equation
    :param second_number: second number to put into an equation
    :param operation: mathematical operation to perform
    """

    # provide a list of valid operations
    valid_operations = ("+", "-", "*", "/")
    # define which fields can use Jinja templating
    template_fields = ("first_number", "second_number")

    def __init__(
        self,
        first_number: float,
        second_number: float,
        operation: str,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.first_number = first_number
        self.second_number = second_number
        self.operation = operation

        # raise an import error if the operation provided is not valid
        if self.operation not in self.valid_operations:
            raise ValueError(
                f"{self.operation} is not a valid operation. Choose one of {self.valid_operations}"
            )

    def execute(self, context):
        self.log.info(
            f"Equation: {self.first_number} {self.operation} {self.second_number}"
        )
        if self.operation == "+":
            res = self.first_number + self.second_number
            self.log.info(f"Result: {res}")
            return res
        if self.operation == "-":
            res = self.first_number - self.second_number
            self.log.info(f"Result: {res}")
            return res
        if self.operation == "*":
            res = self.first_number * self.second_number
            self.log.info(f"Result: {res}")
            return res
        if self.operation == "/":
            try:
                res = self.first_number / self.second_number
            except ZeroDivisionError as err:
                self.log.critical(
                    "If you have set up an equation where you are trying to divide by zero, you have done something WRONG. - Randall Munroe, 2006"
                )
                raise ZeroDivisionError

            self.log.info(f"Result: {res}")
            return res


class MyResumableMathOperator(MyBasicMathOperator):
    """
    Example Operator that stores its result in the task state store before it can
    fail, so a retry returns the stored result instead of recomputing it.
    :param simulate_crash: raise after storing the result to force one retry
    """

    state_key = "result"

    def __init__(self, simulate_crash: bool = False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.simulate_crash = simulate_crash

    def execute(self, context):
        task_state_store = context["task_state_store"]
        res = task_state_store.get(self.state_key)

        if res is not None:
            self.log.info(f"Recovered result from the task state store: {res}")
            return res

        res = super().execute(context)
        task_state_store.set(self.state_key, res)

        if self.simulate_crash:
            raise Exception(
                "Simulated worker crash after the result was stored in the task state store."
            )

        return res