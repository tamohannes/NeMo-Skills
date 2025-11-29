#!/usr/bin/env python3
"""
Script to extract edit locations from problem_statement and create proper edit_locations structure.

This script processes JSONL files that have locations embedded in problem_statement text
and extracts them into a structured edit_locations field.
"""

import argparse
import json
import re
from pathlib import Path


def extract_locations_from_problem_statement(problem_statement: str):
    """
    Extract locations from problem_statement text.
    
    Returns:
        tuple: (clean_problem_statement, locations_list)
        - clean_problem_statement: problem_statement without the locations section
        - locations_list: list of dicts with file_path, start_line, end_line
    """
    # Pattern to match the locations section
    pattern = r'--- EDIT LOCATIONS ---\s*\n(.*?)(?:\n\n|\Z)'
    match = re.search(pattern, problem_statement, re.DOTALL)
    
    if not match:
        return problem_statement, []
    
    locations_text = match.group(1)
    clean_problem_statement = problem_statement[:match.start()].rstrip()
    
    # Extract individual location lines
    # Pattern 1: With line numbers: • file_path: Lstart-Lend
    # Pattern 2: Without line numbers: • file_path
    location_pattern = r'[•\u2022]\s*([^\n:]+)(?::\s*L(\d+)-L(\d+))?'
    
    locations = []
    for line in locations_text.split('\n'):
        line = line.strip()
        if not line:
            continue
        
        match_loc = re.search(location_pattern, line)
        if match_loc:
            file_path = match_loc.group(1).strip()
            start_line = int(match_loc.group(2)) if match_loc.group(2) else None
            end_line = int(match_loc.group(3)) if match_loc.group(3) else None
            
            locations.append({
                "file_path": file_path,
                "start_line": start_line,
                "end_line": end_line
            })
    
    return clean_problem_statement, locations


def process_jsonl_file(input_file: Path, source_type: str = "ground_truth"):
    """
    Process a JSONL file to extract locations and create edit_locations structure.
    
    Args:
        input_file: Path to input JSONL file
        source_type: Type of source ("ground_truth", "ground_truth_wo_lines", "artsiv")
    """
    output_file = input_file  # Overwrite in place
    
    reasoning_map = {
        "ground_truth": "Ground truth locations provided",
        "ground_truth_wo_lines": "Ground truth locations provided",
        "artsiv": "Artsiv predicted locations"
    }
    reasoning = reasoning_map.get(source_type, "Locations provided")
    
    updated_samples = []
    
    with open(input_file, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            if not line.strip():
                continue
            
            try:
                sample = json.loads(line)
                
                # Check if locations are embedded in problem_statement
                problem_statement = sample.get('problem_statement', '')
                clean_problem_statement, locations = extract_locations_from_problem_statement(problem_statement)
                
                # Update problem_statement
                sample['problem_statement'] = clean_problem_statement
                
                # Create edit_locations structure if locations were found
                if locations:
                    sample['edit_locations'] = {
                        "reasoning": reasoning,
                        "locations": locations
                    }
                # If no locations found, ensure edit_locations is not present (for baseline)
                elif 'edit_locations' in sample:
                    del sample['edit_locations']
                
                updated_samples.append(sample)
                
            except json.JSONDecodeError as e:
                print(f"Warning: Failed to parse line {line_num} in {input_file}: {e}")
                continue
    
    # Write updated samples back
    with open(output_file, 'w', encoding='utf-8') as f:
        for sample in updated_samples:
            f.write(json.dumps(sample, ensure_ascii=False) + '\n')
    
    print(f"Processed {len(updated_samples)} samples from {input_file}")
    if updated_samples:
        locations_count = sum(1 for s in updated_samples if 'edit_locations' in s)
        print(f"  - {locations_count} samples with edit_locations")
        print(f"  - {len(updated_samples) - locations_count} samples without edit_locations")


def main():
    parser = argparse.ArgumentParser(description="Extract locations from problem_statement and create edit_locations structure")
    parser.add_argument('--dataset_dir', type=str, 
                       default=str(Path(__file__).parent),
                       help='Directory containing JSONL files')
    parser.add_argument('--files', nargs='+', 
                       choices=['ground_truth', 'ground_truth_wo_lines', 'artsiv', 'all'],
                       default=['all'],
                       help='Which files to process')
    
    args = parser.parse_args()
    dataset_dir = Path(args.dataset_dir)
    
    files_to_process = []
    if 'all' in args.files:
        files_to_process = [
            ('ground_truth.jsonl', 'ground_truth'),
            ('ground_truth_wo_lines.jsonl', 'ground_truth_wo_lines'),
            ('artsiv.jsonl', 'artsiv')
        ]
    else:
        file_map = {
            'ground_truth': ('ground_truth.jsonl', 'ground_truth'),
            'ground_truth_wo_lines': ('ground_truth_wo_lines.jsonl', 'ground_truth_wo_lines'),
            'artsiv': ('artsiv.jsonl', 'artsiv')
        }
        files_to_process = [file_map[f] for f in args.files]
    
    for filename, source_type in files_to_process:
        filepath = dataset_dir / filename
        if not filepath.exists():
            print(f"Warning: {filepath} does not exist, skipping")
            continue
        
        print(f"\nProcessing {filename}...")
        process_jsonl_file(filepath, source_type)


if __name__ == "__main__":
    main()

