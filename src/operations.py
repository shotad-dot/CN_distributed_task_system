import math

SUPPORTED_OPERATIONS = ["ADD", "SUBTRACT", "MULTIPLY", "DIVIDE", "MODULO",
                        "POWER", "FLOOR_DIVIDE", "MAX", "MIN", "GCD", "LCM"]

def execute_operation(operation, a, b):
    op = operation.upper()
    if op == "ADD": return a + b
    if op == "SUBTRACT": return a - b
    if op == "MULTIPLY": return a * b
    if op == "DIVIDE":
        if b == 0: raise ZeroDivisionError("Division by zero")
        return a / b
    if op == "MODULO":
        if b == 0: raise ZeroDivisionError("Modulo by zero")
        return a % b
    if op == "POWER": return a ** b
    if op == "FLOOR_DIVIDE":
        if b == 0: raise ZeroDivisionError("Division by zero")
        return a // b
    if op == "MAX": return max(a, b)
    if op == "MIN": return min(a, b)
    if op in ("GCD", "LCM"):
        if not float(a).is_integer() or not float(b).is_integer():
            raise ValueError(f"{op} requires integer inputs")
        return math.gcd(int(a), int(b)) if op == "GCD" else math.lcm(int(a), int(b))
    raise ValueError(f"Unsupported operation: {op}")
