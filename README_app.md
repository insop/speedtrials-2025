# README APP

Our water quality assistant help Georgia residents to find out the safety of their drinking water.


## Main components:

1. User ask natural language question
- We take the natural language question and parse it using LLM.

For example, if the query is "how's quality of atlanta?", then we can use exiting query.

Or, we can ask "water quality population over 2000?" then we generate SQL query using LLM.

2. Summarize the result in natural language

- We combine query results and use LLM to summarize the results so that users can easily find out the safety without going through the long results