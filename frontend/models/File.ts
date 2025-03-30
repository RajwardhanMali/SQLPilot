import mongoose, { Schema, Document, Types } from "mongoose";

export interface IFile extends Document {
  name: string;
  project_id: Types.ObjectId;
}

const FileSchema = new Schema<IFile>(
  {
    name: { type: String, required: true, minlength: 2, maxlength: 100 },
    project_id: { type: Schema.Types.ObjectId, ref: "Project", required: true },
  },
  { timestamps: true }
);

// Check if model exists before creating
const FileModel =
  mongoose.models.File || mongoose.model<IFile>("File", FileSchema);

export default FileModel;
