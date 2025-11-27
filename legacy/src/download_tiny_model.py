from transformers import AutoTokenizer, AutoModelForCausalLM

# HuggingFace tiny GPT-2 (~6MB)
MODEL = "facebook/opt-125m"
TARGET = "./tiny-model"

print(f"Downloading tiny model: {MODEL}")
tokenizer = AutoTokenizer.from_pretrained(MODEL)
model = AutoModelForCausalLM.from_pretrained(MODEL)

tokenizer.save_pretrained(TARGET)
model.save_pretrained(TARGET)

print(f"Saved tiny test model to: {TARGET}")
