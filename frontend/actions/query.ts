"use server";
import Query from "@/models/Query";
import { revalidatePath } from "next/cache";

export type SerializedQuery = {
  _id: string;
  queryText: string;
  dialect: string;
  createdAt: string;
  error: { message: string; name: string } | null;
  output: {
    columns: string[];
    rows: Array<Array<any>>;
  } | null;
  success: string;
};

export type QueryInput = {
  project: string;
  schemaId: string;
  queryText: string;
  dialect: string;
  outputColumns?: string[];
  outputRows?: any[][];
  errorMessage?: string;
  errorName?: string;
};

export const saveQuery = async (input: QueryInput) => {
  try {
    const query = {
      project: input.project,
      schemaId: input.schemaId,
      queryText: input.queryText,
      dialect: input.dialect,
      output: input.outputColumns
        ? {
            columns: input.outputColumns,
            rows: input.outputRows || [],
          }
        : null,
      error: input.errorMessage
        ? {
            message: input.errorMessage,
            name: input.errorName || "Error",
          }
        : null,
      success: input.errorMessage ? "" : "Query executed successfully",
    };

    await Query.create(query);
    revalidatePath("/dashboard");
  } catch (error) {
    console.error("Error saving query:", error);
    throw error;
  }
};

export const getQueries = async (
  projectId: string
): Promise<SerializedQuery[]> => {
  try {
    console.log("Fetching queries for project:", projectId);
    const result = await Query.find({ project: projectId })
      .sort({ createdAt: -1 })
      .limit(100)
      .lean();

    // Serialize the MongoDB documents
    const serialized = result.map((doc: any) => ({
      _id: doc._id.toString(),
      queryText: doc.queryText,
      dialect: doc.dialect,
      createdAt: doc.createdAt.toISOString(),
      error: doc.error,
      output: doc.output
        ? {
            columns: doc.output.columns,
            rows: doc.output.rows,
          }
        : null,
      success: doc.success,
    }));

    return serialized;
  } catch (error) {
    console.error("Error fetching queries:", error);
    throw error;
  }
};
