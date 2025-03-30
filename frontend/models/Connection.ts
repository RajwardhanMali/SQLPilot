import mongoose, { Schema, Document } from "mongoose";

export interface IConnection extends Document {
  dialect: "Trino" | "Spark";
  database: string;
  host: string;
  username: string;
  port: number;
  password: string;
}

const ConnectionSchema = new Schema<IConnection>(
  {
    dialect: { type: String, required: true, minlength: 2, maxlength: 100 },
    database: { type: String, required: true, unique: true },
    host: { type: String, required: true },
    username: { type: String, required: true },
    port: { type: Number, required: true },
    password: { type: String, required: true },

  },
  { timestamps: true }
);

const Connection =
  mongoose.models.Connection ||
  mongoose.model<IConnection>("Connection", ConnectionSchema);
export default Connection;
