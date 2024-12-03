import sys
if sys.version_info >= (3, 12, 0):
    import six
    sys.modules["kafka.vendor.six.moves"] = six.moves

from kafka import KafkaProducer
import os
import time
import json

# Initialisation du producer Kafka
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Configuration
topic = "audio_segmentation"
audio_folder = "/Users/macbookpro/Documents/COURS/TraitementDuSon/tpnote/archive/Respiratory_Sound_Database/Respiratory_Sound_Database/audio_and_txt_files"
processed_files = set()  # Suivi des fichiers traités

def read_annotation_file(txt_file):
    """Lire un fichier d'annotation .txt et retourner les segments."""
    segments = []
    with open(txt_file, "r") as f:
        for line in f:
            start, end, crackles, wheezes = line.strip().split("\t")
            segments.append({
                "start": float(start),
                "end": float(end),
                "crackles": int(crackles),
                "wheezes": int(wheezes)
            })
    return segments

while True:
    # Parcourir les fichiers dans le dossier
    files = sorted(os.listdir(audio_folder))  # Tri pour une lecture ordonnée
    new_files = [f for f in files if f.endswith(".wav") and f not in processed_files]

    if not new_files:
        print("Aucun nouveau fichier à traiter. En attente...")
        time.sleep(20)  # Attendre avant de vérifier à nouveau
        continue

    # Traiter le premier fichier non traité
    file = new_files[0]
    audio_file = os.path.join(audio_folder, file)
    annotation_file = audio_file.replace(".wav", ".txt")

    if os.path.exists(annotation_file):
        try:
            # Lire les annotations
            annotations = read_annotation_file(annotation_file)

            # Publier les chemins et les annotations sur Kafka
            message = {
                "audio_file": audio_file,
                "annotation_file": annotation_file,
                "annotations": annotations
            }
            producer.send(topic, value=message)
            print(f"Publié sur Kafka : {message}")

            # Marquer le fichier comme traité
            processed_files.add(file)
        except Exception as e:
            print(f"Erreur lors du traitement du fichier {file}: {e}")
    else:
        print(f"Annotation manquante pour {file}. Ignoré.")

    # Attendre 20 secondes avant de traiter le fichier suivant
    time.sleep(20)
