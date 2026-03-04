#!/usr/bin/env python3
"""
Prompt Generator for vLLM Prefix Caching Tests

This script generates various prompt strategies and saves them to JSON files
for later use in stress testing.
"""

import json
import argparse
from datetime import datetime
from typing import List, Dict
from pathlib import Path


class PromptGenerator:
    """Generate different prompt strategies for testing prefix caching"""
    
    def generate_long_prefix_prompts(self, num_requests: int) -> List[str]:
        """
        Generate prompts with LONG shared prefixes (200+ tokens)
        Optimized for block_size=128 with high cache hit rates
        """
        
        # Very long shared prefix (approximately 200-250 tokens)
        long_prefix = """You are an expert AI assistant with deep knowledge in computer science, 
machine learning, and software engineering. When answering questions, please follow these 
comprehensive guidelines to ensure high-quality responses:

1. Start with a clear, concise definition of the main concept
2. Provide historical context and evolution of the technology
3. Explain the underlying principles and theoretical foundations
4. Include real-world examples and practical use cases
5. Discuss advantages and disadvantages objectively
6. Compare with related concepts and alternative approaches
7. Mention common pitfalls, challenges, and best practices
8. Provide code examples or pseudocode where appropriate
9. Explain any technical terminology in accessible language
10. Consider different levels of abstraction from high-level to implementation details
11. Reference relevant research papers or industry standards if applicable
12. Discuss scalability, performance, and production considerations
13. Include debugging tips and troubleshooting strategies
14. Mention tools, libraries, or frameworks commonly used
15. Suggest further resources for deeper learning

Please structure your response with clear sections, use bullet points for lists, 
and ensure your explanation is both technically accurate and accessible to learners 
at different levels. Now, please provide a comprehensive explanation of the following topic:

"""
        
        # Different topics to append to the prefix
        topics = [
            "neural network architectures and their applications in modern AI systems, Model unloading in multi-model serving: Some frameworks can swap models in/out,Kubernetes autoscaling: Scale down pods when idle, scale up on demand ",
            "distributed systems design patterns and consistency models, Is there a specific reason you want sleep mode? Power saving, memory management, or something else? I might be able to suggest an alternative approach. When the vLLM process terminates, the NPU memory is released",
            "database indexing strategies and query optimization techniques, A data set is a collection of data. In the case of tabular data, a data set corresponds to one or more database tables, where every column of a table represents a particular variable, and each row corresponds to a given record of the data set in question. If you're a dataset owner and wish to update any part of it (description, citation, license, etc.), or do not want your dataset to be included in the Hugging Face Hub, please get in touch by opening a discussion or a pull request in the Community tab of the dataset page. Thanks for your contribution to the ML community!",
            "containerization technologies and orchestration platforms",
            "machine learning model training and hyperparameter tuning",
            "natural language processing transformers and attention mechanisms",
            "cloud-native application architecture and microservices patterns",
            "computer vision algorithms and deep learning for image recognition",
            "reinforcement learning algorithms and their real-world applications",
            "graph neural networks and their applications in recommendation systems",
            "time series forecasting with deep learning models",
            "automated machine learning (AutoML) and neural architecture search",
            "federated learning and privacy-preserving machine learning",
            "explainable AI and interpretability in machine learning models",
            "transfer learning and domain adaptation techniques"
        ]
        
        prompts = []
        
        for i in range(num_requests):
            # 70% of requests share the exact same long prefix
            if i % 100 < 2:
                topic = topics[i % len(topics)]
                prompts.append(f"{long_prefix}{topic}")
            else:
                # 30% have slightly different prefixes (to test partial matching)
                short_prefix = "Please explain in detail: "
                topic = topics[i % len(topics)]
                prompts.append(f"{short_prefix}{topic}")
        
        return prompts
    
    def generate_very_long_prefix_prompts(self, num_requests: int) -> List[str]:
        """
        Generate prompts with VERY LONG shared prefixes (400+ tokens)
        For testing extreme prefix caching scenarios
        """
        
        # Extremely long shared prefix (approximately 400-500 tokens)
        very_long_prefix = """You are an advanced AI assistant specializing in technical education 
and knowledge transfer. Your role is to provide comprehensive, well-structured, and pedagogically 
sound explanations of complex topics in computer science, artificial intelligence, and software 
engineering. 

When answering questions, you must adhere to the following detailed guidelines:

STRUCTURE AND ORGANIZATION:
1. Begin with an executive summary (2-3 sentences) that captures the essence of the topic
2. Provide a detailed table of contents outlining the sections you will cover
3. Start with fundamental concepts before progressing to advanced topics
4. Use a hierarchical structure with clear headings and subheadings
5. Ensure logical flow and smooth transitions between sections

CONTENT REQUIREMENTS:
6. Define all key terms and concepts explicitly
7. Provide historical context and the evolution of the technology
8. Explain theoretical foundations with mathematical formulations where appropriate
9. Include at least 3-5 concrete real-world examples
10. Discuss current state-of-the-art and recent developments
11. Compare and contrast with alternative approaches
12. Address common misconceptions and clarify confusing aspects

TECHNICAL DEPTH:
13. Provide implementation details with code examples in relevant languages
14. Explain algorithmic complexity and performance characteristics
15. Discuss scalability considerations and bottlenecks
16. Include architecture diagrams or pseudocode representations
17. Mention specific tools, libraries, frameworks, and their versions

PRACTICAL CONSIDERATIONS:
18. List best practices and design patterns
19. Highlight common pitfalls and how to avoid them
20. Provide debugging strategies and troubleshooting tips
21. Discuss testing approaches and quality assurance
22. Consider security implications and privacy concerns
23. Address deployment and production readiness

LEARNING SUPPORT:
24. Include exercises or thought experiments for deeper understanding
25. Suggest progressive learning paths from beginner to expert
26. Recommend specific books, papers, courses, and online resources
27. Provide analogies and metaphors to clarify abstract concepts
28. Address different learning styles with varied explanations

Now, with all these guidelines in mind, please provide an exhaustive and pedagogically excellent 
explanation of the following topic:

"""
        
        topics = [
            "the transformer architecture in natural language processing, including multi-head attention mechanisms and positional encodings",
            "distributed training of large language models across multiple GPUs using data parallelism and model parallelism",
            "the mathematical foundations of backpropagation and automatic differentiation in neural networks",
            "modern recommendation system architectures including collaborative filtering and deep learning approaches",
            "Kubernetes architecture and container orchestration patterns for production machine learning systems"
        ]
        
        prompts = []
        
        for i in range(num_requests):
            # 80% share the very long prefix
            if i % 10 < 8:
                topic = topics[i % len(topics)]
                prompts.append(f"{very_long_prefix}{topic}")
            else:
                # 20% unique
                topic = topics[i % len(topics)]
                prompts.append(f"[Unique request {i}] Provide a detailed technical explanation of {topic}")
        
        return prompts
    
    def generate_conversation_threads(self, num_requests: int) -> List[str]:
        """
        Generate conversation-style prompts simulating multi-turn dialogues
        Each thread shares a growing prefix
        """
        
        # Multiple conversation threads
        threads = [
            {
                "context": """I am a machine learning engineer working on a recommendation system 
for an e-commerce platform. We have 10 million users and 1 million products. Our current system 
uses collaborative filtering with matrix factorization, but we're experiencing scalability issues 
and cold-start problems with new users and products. Our infrastructure runs on Kubernetes with 
a mix of Python microservices and a PostgreSQL database. We process about 100,000 events per minute.""",
                "questions": [
                    " How should we approach adding deep learning to our recommendation system?",
                    " What neural network architecture would work best for our scale?",
                    " How can we handle the cold-start problem with neural networks?",
                    " What's the best way to train models with our event stream?",
                    " Should we use online learning or batch training?"
                ]
            },
            {
                "context": """I'm developing a computer vision application for medical image analysis, 
specifically for detecting anomalies in X-ray images. We have a dataset of 50,000 labeled images, 
but the class imbalance is severe - only 2% of images contain anomalies. We need to achieve at 
least 95% recall while maintaining reasonable precision. The model will run on edge devices with 
limited computational resources (NVIDIA Jetson Xavier). Inference latency must be under 100ms.""",
                "questions": [
                    " What data augmentation strategies should we use for this imbalanced dataset?",
                    " Which pre-trained model architecture is best for medical X-ray analysis?",
                    " How can we optimize the model for edge deployment on Jetson?",
                    " What loss function works best for highly imbalanced classification?",
                    " How should we handle model uncertainty and provide confidence scores?"
                ]
            },
            {
                "context": """Our team is building a real-time natural language processing system for 
customer support chat. We receive about 10,000 concurrent conversations during peak hours. The system 
needs to classify user intents, extract entities, and route to appropriate agents. We're currently 
using BERT-base, but inference latency is around 200ms per message, which is too slow. We need to 
reduce latency to under 50ms while maintaining accuracy above 90%. Our infrastructure uses AWS with 
EKS and we have GPU instances available.""",
                "questions": [
                    " How can we reduce BERT inference latency for real-time chat?",
                    " Should we consider knowledge distillation or model quantization?",
                    " What are the trade-offs between different model compression techniques?",
                    " How can we implement efficient batching for variable-length sequences?",
                    " What monitoring and A/B testing strategy should we use for model updates?"
                ]
            }
        ]
        
        prompts = []
        
        for i in range(num_requests):
            thread_idx = i % len(threads)
            thread = threads[thread_idx]
            
            # Cycle through questions in each thread
            question_idx = (i // len(threads)) % len(thread["questions"])
            
            # Build cumulative context (simulating conversation history)
            context = thread["context"]
            
            # Add previous questions to context (shared prefix grows)
            for j in range(question_idx):
                context += thread["questions"][j]
            
            # Add current question
            current_question = thread["questions"][question_idx]
            full_prompt = f"{context}{current_question}"
            
            prompts.append(full_prompt)
        
        return prompts
    
    def generate_diverse_prompts(self, num_requests: int) -> List[str]:
        """Diverse prompts with minimal caching (baseline)"""
        prefixes = [
            "Explain the concept of",
            "What are the benefits of",
            "How does one implement",
        ]
        
        topics = [
            "machine learning", "neural networks", "deep learning",
            "NLP", "computer vision", "reinforcement learning"
        ]
        
        prompts = []
        for i in range(num_requests):
            prefix = prefixes[i % len(prefixes)]
            topic = topics[i % len(topics)]
            prompts.append(f"{prefix} {topic}? [Request {i}]")
        
        return prompts
    
    def analyze_prompts(self, prompts: List[str], strategy: str) -> Dict:
        """Analyze prompt characteristics"""
        if not prompts:
            return {}
        
        # Basic statistics
        total_chars = sum(len(p) for p in prompts)
        avg_chars = total_chars / len(prompts)
        avg_tokens = avg_chars / 4  # Rough estimate
        
        # Analyze prefix sharing between first two prompts
        shared_prefix_chars = 0
        if len(prompts) >= 2:
            for c1, c2 in zip(prompts[0], prompts[1]):
                if c1 == c2:
                    shared_prefix_chars += 1
                else:
                    break
        
        shared_prefix_tokens = shared_prefix_chars / 4
        
        analysis = {
            'strategy': strategy,
            'total_prompts': len(prompts),
            'avg_characters': avg_chars,
            'estimated_avg_tokens': avg_tokens,
            'min_characters': min(len(p) for p in prompts),
            'max_characters': max(len(p) for p in prompts),
            'shared_prefix_chars': shared_prefix_chars,
            'estimated_shared_tokens': shared_prefix_tokens,
            'cache_potential': self._assess_cache_potential(shared_prefix_tokens)
        }
        
        return analysis
    
    def _assess_cache_potential(self, shared_tokens: float) -> str:
        """Assess caching potential based on shared prefix length"""
        if shared_tokens > 128:
            return "EXCELLENT - Shared prefix exceeds block_size=128"
        elif shared_tokens > 64:
            return "GOOD - Shared prefix exceeds 64 tokens"
        elif shared_tokens > 16:
            return "MODERATE - Shared prefix is 16-64 tokens"
        else:
            return "LOW - Shared prefix < 16 tokens, cache may not help"
    
    def save_prompts(self, prompts: List[str], strategy: str, output_file: str = None):
        """Save prompts to JSON file with metadata"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create prompts directory if it doesn't exist
        prompts_dir = Path("prompts")
        prompts_dir.mkdir(exist_ok=True)
        
        if output_file is None:
            output_file = prompts_dir / f"prompts_{strategy}_{timestamp}.json"
        else:
            # If custom output file specified, still put it in prompts folder
            output_file = prompts_dir / Path(output_file).name
        
        # Analyze prompts
        analysis = self.analyze_prompts(prompts, strategy)
        
        # Create data structure
        data = {
            'metadata': {
                'strategy': strategy,
                'generated_at': datetime.now().isoformat(),
                'num_prompts': len(prompts),
                'analysis': analysis
            },
            'prompts': prompts
        }
        
        # Save to file
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        return str(output_file), analysis


def print_analysis(analysis: Dict):
    """Pretty print prompt analysis"""
    print("\n" + "="*80)
    print(" PROMPT ANALYSIS")
    print("="*80)
    print(f"Strategy: {analysis['strategy']}")
    print(f"Total prompts: {analysis['total_prompts']}")
    print(f"Average length: {analysis['avg_characters']:.0f} chars (~{analysis['estimated_avg_tokens']:.0f} tokens)")
    print(f"Length range: {analysis['min_characters']}-{analysis['max_characters']} chars")
    print(f"\nPrefix Sharing:")
    print(f"  Shared prefix: {analysis['shared_prefix_chars']} chars (~{analysis['estimated_shared_tokens']:.0f} tokens)")
    print(f"  Cache potential: {analysis['cache_potential']}")
    print("="*80 + "\n")


def main():
    parser = argparse.ArgumentParser(
        description='Generate prompts for vLLM prefix caching stress tests',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Available strategies:
  long_prefix          - 200+ token shared prefix (70% cache hit expected)
  very_long_prefix     - 400+ token shared prefix (80% cache hit expected)
  conversation_threads - Multi-turn conversations with growing context
  diverse              - Minimal caching (baseline comparison)

Examples:
  # Generate long prefix prompts (saved to prompts/ folder)
  python generate_prompts.py -s long_prefix -n 200

  # Generate multiple strategies
  python generate_prompts.py -s long_prefix very_long_prefix diverse -n 200

  # Specify custom output file (still saved to prompts/ folder)
  python generate_prompts.py -s long_prefix -n 100 -o my_prompts.json

Note: All generated files are saved to the 'prompts/' directory
        """
    )
    
    parser.add_argument(
        '-s', '--strategy',
        nargs='+',
        choices=['long_prefix', 'very_long_prefix', 'conversation_threads', 'diverse'],
        default=['long_prefix'],
        help='Prompt generation strategy (can specify multiple)'
    )
    
    parser.add_argument(
        '-n', '--num-requests',
        type=int,
        default=600,
        help='Number of prompts to generate (default: 200)'
    )
    
    parser.add_argument(
        '-o', '--output',
        type=str,
        help='Output filename (default: prompts/{strategy}_{timestamp}.json)'
    )
    
    args = parser.parse_args()
    
    generator = PromptGenerator()
    
    print("="*80)
    print(" PROMPT GENERATOR FOR VLLM PREFIX CACHING TESTS")
    print("="*80)
    
    for strategy in args.strategy:
        print(f"\n Generating prompts for strategy: {strategy}")
        print(f"   Number of prompts: {args.num_requests}")
        
        # Generate prompts based on strategy
        if strategy == 'long_prefix':
            prompts = generator.generate_long_prefix_prompts(args.num_requests)
        elif strategy == 'very_long_prefix':
            prompts = generator.generate_very_long_prefix_prompts(args.num_requests)
        elif strategy == 'conversation_threads':
            prompts = generator.generate_conversation_threads(args.num_requests)
        elif strategy == 'diverse':
            prompts = generator.generate_diverse_prompts(args.num_requests)
        else:
            print(f" Unknown strategy: {strategy}")
            continue
        
        # Save prompts
        output_file = args.output if len(args.strategy) == 1 else None
        saved_file, analysis = generator.save_prompts(prompts, strategy, output_file)
        
        print(f" Saved to: {saved_file}")
        
        # Print analysis
        print_analysis(analysis)
        
        # Show sample prompts
        print(" Sample prompts (first 3):")
        for i, prompt in enumerate(prompts[:3]):
            preview = prompt[:150] + "..." if len(prompt) > 150 else prompt
            print(f"\n  Prompt {i}:")
            print(f"    Length: {len(prompt)} chars (~{len(prompt)//4} tokens)")
            print(f"    Preview: {preview}")
    
    print("\n" + "="*80)
    print(" Prompt generation complete!")
    print("="*80 + "\n")


if __name__ == "__main__":
    main()