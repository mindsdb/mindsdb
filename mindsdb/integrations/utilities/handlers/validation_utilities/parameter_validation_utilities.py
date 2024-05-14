import difflib


class ParameterValidationUtilities:
    @staticmethod
    def validate_parameter_spelling(handler_cls, parameters):
        expected_params = handler_cls.model_fields.keys()
        for key in parameters.keys():
            if key not in expected_params:
                close_matches = difflib.get_close_matches(
                    key, expected_params, cutoff=0.4
                )
                if close_matches:
                    raise ValueError(
                        f"Unexpected parameter '{key}'. Did you mean '{close_matches[0]}'?"
                    )
                else:
                    raise ValueError(f"Unexpected parameter '{key}'.")
