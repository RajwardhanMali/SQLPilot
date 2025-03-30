// lib/gemini-sql-generator.ts
import { GoogleGenerativeAI } from "@google/generative-ai";

// Interface for the SQL generation response
export interface SQLGenerationResult {
  query: string;
  explanation?: string;
  error?: string;
}

class GeminiSQLGenerator {
  private genAI: GoogleGenerativeAI;

  constructor(apiKey: string) {
    if (!apiKey) {
      throw new Error("Gemini API key is required");
    }
    this.genAI = new GoogleGenerativeAI(apiKey);
  }

  async generateSQL(
    naturalLanguageQuery: string
  ): Promise<SQLGenerationResult> {
    try {
      // Comprehensive prompt for SQL generation
      const prompt = `
        You are an expert SQL query generator for Trino/Spark SQL in an e-commerce context.
        Convert the natural language request into a precise, optimized SQL query.

        Natural Language Query: "${naturalLanguageQuery}"

        Requirements:
        - Use standard SQL syntax compatible with Trino/Spark
        - Assume a schema with tables: orders, order_items, products, categories
        - Include clear column selections and appropriate joins
        - Use meaningful table and column aliases
        - Add a comment explaining the query's purpose
        - Handle date ranges and aggregations appropriately
        - Optimize for readability and performance

        Respond ONLY with the SQL query. Do not include any additional text or explanations.
      `;

      const model = this.genAI.getGenerativeModel({
        model: "gemini-2.0-flash",
      });
      const result = await model.generateContent(prompt);
      const response = await result.response;
      const sqlQuery = response.text().trim();

      return {
        query: sqlQuery,
        explanation: "SQL query generated successfully",
      };
    } catch (error) {
      console.error("Error generating SQL with Gemini:", error);
      return {
        query: "",
        error:
          error instanceof Error ? error.message : "Unknown error occurred",
      };
    }
  }

  // Optional: Add method for query explanation
  async explainQuery(sqlQuery: string): Promise<string> {
    try {
      const prompt = `
        Provide a detailed, technical explanation of the following SQL query:
        
        ${sqlQuery}

        Explanation should cover:
        - Purpose of the query
        - Key joins and filtering conditions
        - Aggregation logic
        - Performance considerations
      `;

      const model = this.genAI.getGenerativeModel({
        model: "gemini-2.0-flash",
      });
      const result = await model.generateContent(prompt);
      const response = await result.response;

      return response.text().trim();
    } catch (error) {
      console.error("Error explaining SQL query:", error);
      return "Unable to generate explanation.";
    }
  }

  async getCompletion(
    partialQuery: string,
    schema: any
  ): Promise<string | null> {
    try {
      const prompt = `
        Complete this partial SQL query based on the schema context.
        Schema: ${JSON.stringify(schema)}
        Partial query: "${partialQuery}"

        Rules:
        - Only provide the completion part
        - Keep it contextually relevant
        - Be concise and precise
        - Consider table relationships

        Return ONLY the completion text.
      `;

      const model = this.genAI.getGenerativeModel({
        model: "gemini-2.0-flash",
      });
      const result = await model.generateContent(prompt);
      return result.response.text().trim();
    } catch (error) {
      console.error("Completion error:", error);
      return null;
    }
  }
}

// Export a configured instance or a factory function
export const createGeminiSQLGenerator = () => {
  const apiKey = process.env.NEXT_PUBLIC_GEMINI_API_KEY;
  if (!apiKey) {
    throw new Error("Gemini API key is not configured");
  }
  return new GeminiSQLGenerator(apiKey);
};

export async function fetchQuerySuggestions(query: string): Promise<string[]> {
  try {
    const response = await fetch("http://10.1.50.35:5000/query_suggestions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ query }),
    });

    if (!response.ok) {
      throw new Error("Failed to fetch query suggestions");
    }

    const data = await response.json();
    return data.suggestions || [];
  } catch (error) {
    console.error("Error fetching query suggestions:", error);
    return [];
  }
}
