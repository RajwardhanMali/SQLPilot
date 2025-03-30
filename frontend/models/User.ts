import mongoose, { Schema, Document, Types } from "mongoose";

export interface IUser extends Document {
  name: string;
  email: string;
  image: string;
  password?: string;
  connections: Types.ObjectId[];
}

const UserSchema = new Schema<IUser>(
  {
    name: { type: String, required: true, minlength: 2, maxlength: 100 },
    email: { type: String, required: true, unique: true },
    image: { type: String, required: true },
    password: { type: String, select: false }, 
    connections: [{ type: Schema.Types.ObjectId, ref: "Connection" }],
  },
  { timestamps: true }
);

const User = mongoose.models.User || mongoose.model<IUser>("User", UserSchema);
export default User;
