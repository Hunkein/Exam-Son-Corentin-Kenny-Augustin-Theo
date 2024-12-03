import sys
if sys.version_info >= (3, 12, 0):
    import six
    sys.modules["kafka.vendor.six.moves"] = six.moves

from kafka import KafkaConsumer
import pandas as pd
import librosa
import soundfile as sf
import json
import os
import psycopg2

# Configuration Kafka
topic = "audio_segmentation"
consumer = KafkaConsumer(
    topic,
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

# Chemins des fichiers
segments_folder = "/Users/macbookpro/Documents/COURS/TraitementDuSon/tpnote/audioSeg"
os.makedirs(segments_folder, exist_ok=True)

diagnosis_file = "/Users/macbookpro/Documents/COURS/TraitementDuSon/tpnote/archive/Respiratory_Sound_Database/Respiratory_Sound_Database/patient_diagnosis.csv"
demographics_path = "/Users/macbookpro/Documents/COURS/TraitementDuSon/tpnote/archive/demographic_info.txt"
output_csv_path = "segments_metadata_with_features.csv"

# Charger les fichiers patient_diagnosis.csv et demographic_info.txt
diagnosis_df = pd.read_csv(diagnosis_file, names=["Patient ID", "Diagnosis"])
demographics_df = pd.read_csv(
    demographics_path,
    sep=r'\s+',
    names=["Patient ID", "Age", "Sex", "BMI", "Child Weight", "Child Height"],
    usecols=["Patient ID", "Age", "Sex"]
)

# Convertir les colonnes pour éviter les problèmes de fusion
diagnosis_df["Patient ID"] = pd.to_numeric(diagnosis_df["Patient ID"], errors="coerce")
demographics_df["Patient ID"] = pd.to_numeric(demographics_df["Patient ID"], errors="coerce")

# Initialisation du DataFrame
metadata_columns = [
    "Segment File", "Patient ID", "Start (s)", "End (s)", "Crackles", "Wheezes", "Diagnosis",
    "Age", "Sex", "MFCC Mean", "RMS", "Spectral Centroid", "ZCR"
]
metadata_df = pd.DataFrame(columns=metadata_columns)

# Connexion PostgreSQL
def get_db_connection():
    return psycopg2.connect(
        dbname="airflow",  # Nom de la base de données
        user="airflow",    # Nom d'utilisateur
        password="airflow",  # Mot de passe
        host="localhost",
        port="5432"
    )

# Fonction pour insérer les données dans PostgreSQL
def save_to_postgres(metadata_df):
    conn = get_db_connection()
    cursor = conn.cursor()

    insert_query = """
    INSERT INTO audio_metadata (
        segment_file, patient_id, start_time, end_time, crackles, wheezes,
        diagnosis, age, sex, mfcc_mean, rms, spectral_centroid, zcr
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    ) ON CONFLICT DO NOTHING;
    """

    for _, row in metadata_df.iterrows():
        # Convertir crackles et wheezes en booléens
        crackles = bool(row["Crackles"]) if pd.notnull(row["Crackles"]) else None
        wheezes = bool(row["Wheezes"]) if pd.notnull(row["Wheezes"]) else None

        cursor.execute(insert_query, (
            row["Segment File"],
            row["Patient ID"],
            row["Start (s)"],
            row["End (s)"],
            crackles,
            wheezes,
            row["Diagnosis"],
            row["Age"],
            row["Sex"],
            row["MFCC Mean"],
            row["RMS"],
            row["Spectral Centroid"],
            row["ZCR"]
        ))

    conn.commit()
    cursor.close()
    conn.close()


# Fonction pour segmenter l'audio et calculer les caractéristiques
def extract_segments_with_features(audio_path, annotations, patient_id, output_folder):
    try:
        # Charger l'audio
        y, sr = librosa.load(audio_path, sr=None)
        segments_metadata = []

        for idx, segment in enumerate(annotations):
            start_sample = int(segment["start"] * sr)
            end_sample = int(segment["end"] * sr)
            segment_audio = y[start_sample:end_sample]

            # Construire le nom et le chemin du fichier segmenté
            segment_file = f"{os.path.basename(audio_path).replace('.wav', '')}_segment_{idx+1}_{segment['start']}-{segment['end']}.wav"
            segment_path = os.path.join(output_folder, segment_file)

            # Sauvegarder le segment
            sf.write(segment_path, segment_audio, sr)
            print(f"Segment sauvegardé : {segment_path}")

            # Calculer les caractéristiques audio
            mfcc_mean, rms, spectral_centroid, zcr = None, None, None, None
            if len(segment_audio) > 0:
                mfcc = librosa.feature.mfcc(y=segment_audio, sr=sr, n_mfcc=13)
                mfcc_mean = mfcc.mean(axis=1).tolist()
                rms = librosa.feature.rms(y=segment_audio).mean()
                spectral_centroid = librosa.feature.spectral_centroid(y=segment_audio, sr=sr).mean()
                zcr = librosa.feature.zero_crossing_rate(y=segment_audio).mean()

            # Récupérer les métadonnées disponibles
            diagnosis = diagnosis_df.loc[diagnosis_df["Patient ID"] == patient_id, "Diagnosis"].values[0] if patient_id in diagnosis_df["Patient ID"].values else "Unknown"
            age = demographics_df.loc[demographics_df["Patient ID"] == patient_id, "Age"].values[0] if patient_id in demographics_df["Patient ID"].values else None
            sex = demographics_df.loc[demographics_df["Patient ID"] == patient_id, "Sex"].values[0] if patient_id in demographics_df["Patient ID"].values else None

            # Ajouter les métadonnées et les caractéristiques
            segment_metadata = {
                "Segment File": segment_file,
                "Patient ID": patient_id,
                "Start (s)": segment["start"],
                "End (s)": segment["end"],
                "Crackles": segment["crackles"],
                "Wheezes": segment["wheezes"],
                "Diagnosis": diagnosis,
                "Age": age,
                "Sex": sex,
                "MFCC Mean": mfcc_mean,
                "RMS": rms,
                "Spectral Centroid": spectral_centroid,
                "ZCR": zcr
            }
            segments_metadata.append(segment_metadata)

        return segments_metadata

    except Exception as e:
        print(f"Erreur lors de la segmentation de {audio_path}: {e}")
        return []

# Traiter les messages Kafka
for message in consumer:
    data = message.value
    audio_file = data["audio_file"]
    annotations = data["annotations"]

    # Extraire l'ID du patient à partir du nom du fichier
    patient_id = int(os.path.basename(audio_file).split("_")[0])

    print(f"Message reçu pour {audio_file} avec {len(annotations)} segments")

    # Segmenter l'audio et enrichir avec les métadonnées et les caractéristiques
    segments_metadata = extract_segments_with_features(audio_file, annotations, patient_id, segments_folder)

    if segments_metadata:
        # Ajouter les segments dans metadata_df
        new_metadata_df = pd.DataFrame(segments_metadata)
        new_metadata_df = new_metadata_df.dropna(how='all', axis=1)
        metadata_df = pd.concat([metadata_df, new_metadata_df], ignore_index=True)

        # Insérer dans PostgreSQL
        save_to_postgres(new_metadata_df)

    # Sauvegarder les métadonnées mises à jour dans un fichier CSV
    metadata_df.to_csv(output_csv_path, index=False)
    print(f"Métadonnées avec caractéristiques ajoutées. Fichier sauvegardé dans : {output_csv_path}")
