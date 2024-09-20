

# Galois Field Operations
class GF:
    def __init__(self, primitive_polynomial=0x11d, field_size=256):
        self.field_size = field_size
        self.primitive_polynomial = primitive_polynomial
        self.exp_table = [0] * (2 * field_size)
        self.log_table = [0] * field_size
        self._init_tables()

    def _init_tables(self):
        x = 1
        for i in range(self.field_size - 1):
            self.exp_table[i] = x
            self.log_table[x] = i
            x <<= 1
            if x & self.field_size:
                x ^= self.primitive_polynomial
        for i in range(self.field_size - 1, 2 * self.field_size - 2):
            self.exp_table[i] = self.exp_table[i - (self.field_size - 1)]

    def add(self, x, y):
        return x ^ y

    def sub(self, x, y):
        return x ^ y

    def mul(self, x, y):
        if x == 0 or y == 0:
            return 0
        return self.exp_table[(self.log_table[x] + self.log_table[y]) % (self.field_size - 1)]

    def div(self, x, y):
        if y == 0:
            raise ZeroDivisionError()
        if x == 0:
            return 0
        return self.exp_table[(self.log_table[x] - self.log_table[y]) % (self.field_size - 1)]

    def exp(self, x):
        return self.exp_table[x % (self.field_size - 1)]