from techchallenge.extract.streaming.streaming_consumer import StreamingConsumer
from techchallenge.pipeline import (
    SilverPipeline,
    GoldPipeline
)


class StreamingPipeline:

    def __init__(self):

        self.consumer = StreamingConsumer()
        self.silver = SilverPipeline()
        self.gold = GoldPipeline()

    def run(self):

        print("=" * 80)
        print("STREAMING PIPELINE")
        print("=" * 80)

        self.consumer.run()

        self.silver.run()

        self.gold.run()

        print("\nStreaming finalizado.")