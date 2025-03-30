import mongoose, { Document, Types } from "mongoose";

export interface ISchema extends Document {
  name: string;
  ddl: string;
  project: Types.ObjectId;
  mermaid: string;
}

const schemaSchema = new mongoose.Schema<ISchema>(
  {
    name: { type: String, required: true, minlength: 3 },
    ddl: { type: String, required: true },
    project: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "Project",
      required: true,
    },
    mermaid: { type: String, required: true },
  },
  { timestamps: true }
);

const SchemaModel =
  mongoose.models.Schema || mongoose.model<ISchema>("Schema", schemaSchema);
export default SchemaModel;
