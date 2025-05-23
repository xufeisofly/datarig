from tokenizer import Tokenizer
from transformers import AutoTokenizer

text = """\
测试内容æ alphabet õ
"""


if __name__ == '__main__':
  tok_hf = AutoTokenizer.from_pretrained(".")
  tok_tt = Tokenizer()

  print(tok_hf.decode(tok_hf.encode(text)) == tok_tt.decode(tok_tt.encode(text)))
  print(tok_hf.bos_token + text == tok_tt.decode(tok_tt.encode(text)))
