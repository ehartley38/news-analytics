import findspark
findspark.init()
from sparknlp.pretrained import ResourceDownloader
import sparknlp


# # Download and save the NER pipeline
# ResourceDownloader.downloadModel("onto_recognize_entities_sm", "en")

# # Download and save the Lemmatizer model
# ResourceDownloader.downloadModel("lemma", "en")

print(sparknlp.version())