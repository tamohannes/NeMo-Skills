#!/bin/bash

MODEL_SIZE_NUM=480
# RUN_ID="try1_5"
# RUN_ID="try2_4_w_gt_locations"
# RUN_ID="try1_1_w_thinking_locations_w_edit_locations"
RUN_ID="t2_2_w"
# RUN_ID="try1_1_w_thinking_locations_ass_told"
MODEL_SIZE="${MODEL_SIZE_NUM}b"
# MODEL_PATH=/hf_models/sharded_0.7.0_sglang/Qwen3-235B-A22B-Thinking-2507-tp16/
MODEL_PATH=/hf_models/Qwen3-Coder-480B-A35B-Instruct-tp16-sglang
# OUTPUT_DIR="/lustre/fsw/portfolios/llmservice/users/htamoyan/locagent/output_eval/e2e_swe_bench_verified/qwen3_${MODEL_SIZE_NUM}b_swe_bench_artsiv_dev_${RUN_ID}"
OUTPUT_DIR="/lustre/fsw/portfolios/llmservice/users/htamoyan/locagent/output_eval/e2e_swe_bench_verified/${RUN_ID}"


NEMO_SKILLS_DISABLE_UNCOMMITTED_CHANGES_CHECK=1 ns eval \
    --cluster=cw-dfw \
    --server_type=vllm \
    --model "${MODEL_PATH}" \
    --server_args="--enable-auto-tool-choice --tool-call-parser qwen3_coder --load-format sharded_state --tensor-parallel-size 16" \
    --server_nodes=2 \
    --server_gpus=8 \
    --benchmarks=swe-bench \
    --expname="e2e_${RUN_ID}" \
    --output_dir "$OUTPUT_DIR" \
    --split=v2_w_thinking_locations \
    --dependent_jobs=1 \
    --num_chunks=10 \
    --server_container=/lustre/fsw/portfolios/llmservice/users/nliudvig/images/dockerhub-vllm-openai-0.10.0.sqsh \
    ++agent_framework=openhands \
    ++agent_framework_repo=https://github.com/tamohannes/OpenHands.git \
    ++agent_framework_commit=915531fe5c959ff31b7858a7803116a59626c147 \
    ++inference.temperature=0.7 \
    ++inference.top_p=0.8 \
    ++inference.top_k=20
