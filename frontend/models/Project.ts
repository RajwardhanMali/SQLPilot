import mongoose, { Schema, Document, Types } from "mongoose";

export interface IProject extends Document {
  title: string;
  description: string;
  createdBy: Types.ObjectId;
  connection: Types.ObjectId;
  catalogs: String[];
}

const ProjectSchema = new Schema<IProject>(
  {
    title: { type: String, required: true, minlength: 3 },
    description: { type: String, required: true },
    createdBy: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "User",
      required: true,
    },
  },
  { timestamps: true }
);

const Project =
  mongoose.models.Project || mongoose.model<IProject>("Project", ProjectSchema);
export default Project;
