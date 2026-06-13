from model_utils import MODEL_PATH, ensure_model

if __name__ == "__main__":
    print("Starting model training...")
    ensure_model(MODEL_PATH)
    print(f"Successfully trained and saved model to {MODEL_PATH}")
