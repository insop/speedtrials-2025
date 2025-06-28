# README APP

Our water quality assistant helps Georgia residents find out the safety of their drinking water.

## Main Components:

1. **Natural Language Query Processing**
   - We take natural language questions and parse them using LLM.
   - For simple queries like "how's quality of atlanta?", we use existing query patterns.
   - For complex queries like "water quality population over 2000?", we generate SQL queries using LLM.

2. **Intelligent Result Summarization**
   - We combine query results and use LLM to summarize the findings in natural language.
   - This allows users to easily understand water safety without navigating through lengthy technical results.