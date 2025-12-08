from torchvision import transforms
import requests
from PIL import Image
import numpy as np
import onnxruntime as ort
import onnx

def prepare_image(img, target_size):
    if img.mode != 'RGB':
        img = img.convert('RGB')
    img = img.resize(target_size, Image.NEAREST)
    return img

def download_image(url):
    img = Image.open(requests.get(url, stream=True).raw)
    img = prepare_image(img, target_size=(200, 200))
    return img

def transform_image(img):
    preprocess = transforms.Compose([
        transforms.Resize((200, 200)),
        transforms.ToTensor(),
        transforms.Normalize(
            mean=[0.485, 0.456, 0.406],
            std=[0.229, 0.224, 0.225]
        ) # ImageNet normalization
    ])

    x = preprocess(img)
    return x

def predict_image(x):
    with open("imagenet_classes.txt", "r") as f:
        categories = [s.strip() for s in f.readlines()]
    session = ort.InferenceSession(
        "hair_classifier_empty.onnx", providers=["CPUExecutionProvider"]
    )
    input_name = session.get_inputs()[0].name
    output_name = session.get_outputs()[0].name
    result = session.run([output_name], {input_name: x.unsqueeze(0).numpy().astype(np.float32)})
    float_predictions = result[0][0].tolist()
    return float_predictions

def lambda_handler(event, context):
    url = event['url']
    img = download_image(url)
    img = transform_image(img)
    prd = predict_image(img)
    return prd
