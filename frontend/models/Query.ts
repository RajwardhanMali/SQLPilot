import mongoose, { Schema, Document, Types } from "mongoose";

export interface Main {
  error: QueryError | null;
  output: Output | null;
  success: string;
}

export interface QueryError {
  message: string;
  name: string;
}

export interface Output {
  columns: string[];
  cpu_time_seconds: null;
  execution_time_seconds: number;
  peak_memory_bytes: null;
  results: Result[];
  rows: Array<Array<number | string>>;
  trino_elapsed_time_seconds: null;
}

export interface Result {
  email: string;
  registration_date: string;
  user_id: number;
  username: string;
}

export interface IQuery extends Document, Main {
  project: Types.ObjectId;
  schemaId: Types.ObjectId;
  queryText: string;
  dialect: string;
}

const QuerySchema = new Schema<IQuery>(
  {
    output: {
      columns: [String],
      cpu_time_seconds: { type: Number, default: null },
      execution_time_seconds: Number,
      peak_memory_bytes: { type: Number, default: null },
      results: [
        {
          email: String,
          registration_date: String,
          user_id: Number,
          username: String,
        },
      ],
      rows: [[Schema.Types.Mixed]],
      trino_elapsed_time_seconds: { type: Number, default: null },
    },
    project: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "Project",
      required: true,
    },
    schemaId: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "Schema",
      required: true,
    },
    queryText: { type: String, required: true },
    dialect: { type: String, required: true },
    error: {
      message: String,
      name: String,
    },
    success: { type: String },
  },
  { timestamps: true }
);

const Query =
  mongoose.models.Query || mongoose.model<IQuery>("Query", QuerySchema);
export default Query;
