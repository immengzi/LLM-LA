python main.py --mode pull-batching --config lmsys_token_length_false --prompts-limit 100000
sleep 30
python main.py --mode pull-batching --config lmsys_token_length_false_det_32 --prompts-limit 100000
sleep 30
python main.py --mode pull-batching --config lmsys_token_length_false_det_16 --prompts-limit 100000
sleep 30
python main.py --mode pull-batching --config lmsys_token_length_false_det_8 --prompts-limit 100000
sleep 30
python main.py --mode pull-batching --config lmsys_token_length_false_det_4 --prompts-limit 100000
sleep 30

python main.py --mode pull-batching --config lmsys_token_length_true --prompts-limit 100000
sleep 30
python main.py --mode pull-batching --config lmsys_token_length_true_det_32 --prompts-limit 100000
sleep 30
python main.py --mode pull-batching --config lmsys_token_length_true_det_16 --prompts-limit 100000
sleep 30
python main.py --mode pull-batching --config lmsys_token_length_true_det_8 --prompts-limit 100000
sleep 30
python main.py --mode pull-batching --config lmsys_token_length_true_det_4 --prompts-limit 100000
sleep 30

python main.py --mode least-queue-batching --config lmsys_token_length_false --prompts-limit 100000
sleep 30
python main.py --mode least-queue-batching --config lmsys_token_length_false_det_32 --prompts-limit 100000
sleep 30
python main.py --mode least-queue-batching --config lmsys_token_length_false_det_16 --prompts-limit 100000
sleep 30
python main.py --mode least-queue-batching --config lmsys_token_length_false_det_8 --prompts-limit 100000
sleep 30
python main.py --mode least-queue-batching --config lmsys_token_length_false_det_4 --prompts-limit 100000
sleep 30

python main.py --mode least-queue-batching --config lmsys_token_length_true --prompts-limit 100000
sleep 30
python main.py --mode least-queue-batching --config lmsys_token_length_true_det_32 --prompts-limit 100000
sleep 30
python main.py --mode least-queue-batching --config lmsys_token_length_true_det_16 --prompts-limit 100000
sleep 30
python main.py --mode least-queue-batching --config lmsys_token_length_true_det_8 --prompts-limit 100000
sleep 30
python main.py --mode least-queue-batching --config lmsys_token_length_true_det_4 --prompts-limit 100000
sleep 30


python main.py --mode rr-batching --config lmsys_token_length_false --prompts-limit 100000
sleep 30
python main.py --mode rr-batching --config lmsys_token_length_false_det_32 --prompts-limit 100000
sleep 30
python main.py --mode rr-batching --config lmsys_token_length_false_det_16 --prompts-limit 100000
sleep 30
python main.py --mode rr-batching --config lmsys_token_length_false_det_8 --prompts-limit 100000
sleep 30
python main.py --mode rr-batching --config lmsys_token_length_false_det_4 --prompts-limit 100000
sleep 30

python main.py --mode rr-batching --config lmsys_token_length_true --prompts-limit 100000
sleep 30
python main.py --mode rr-batching --config lmsys_token_length_true_det_32 --prompts-limit 100000
sleep 30
python main.py --mode rr-batching --config lmsys_token_length_true_det_16 --prompts-limit 100000
sleep 30
python main.py --mode rr-batching --config lmsys_token_length_true_det_8 --prompts-limit 100000
sleep 30
python main.py --mode rr-batching --config lmsys_token_length_true_det_4 --prompts-limit 100000
sleep 30