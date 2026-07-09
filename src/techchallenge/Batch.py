from techchallenge.pipeline import (
    BronzePipeline,
    SilverPipeline,
    GoldPipeline
)

class BatchPipeline:

    def __init__(self):

        self.bronze = BronzePipeline()
        self.silver = SilverPipeline()
        self.gold = GoldPipeline()

    def run(self):

        print("========== BRONZE ==========")
        self.bronze.run()

        print("========== SILVER ==========")
        self.silver.run()

        print("========== GOLD ==========")
        self.gold.run()

        print("Pipeline Batch concluída.")