from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.stream_execution_environment import StreamExecutionEnvironment
from pyflink.datastream.functions import MapFunction


class MyMapFunction(MapFunction):
    def map(self, value):
        return int(value) * 4


# Set up the execution environment
env = StreamExecutionEnvironment.get_execution_environment()

env.set_parallelism(1)
env.disable_operator_chaining()

# Example data
data = env.from_collection(["1", "2", "3", "4"])

# Apply the map function using a class-based approach
result = data.map(MyMapFunction())

# Collect results
result.print()

try:
    env.execute("Python Map Function Example")
except Exception as e:
    print(f"Execution error: {e}")
