class ProcessorTopology(object):

    def __init__(self, processor_nodes, source_by_topics, state_store_suppliers):
        self.processor_nodes = processor_nodes
        self.source_by_topics = source_by_topics
        self.state_store_suppliers = state_store_suppliers

    def sourceTopics(self):
        return set(self.source_by_topics)

    def source(self, topic):
        return self.source_by_topics.get(topic)

    def sources(self):
        return set(self.source_by_topics.values())

    def processors(self):
        return self.processor_nodes

    def state_store_suppliers(self):
        return self.state_store_suppliers
