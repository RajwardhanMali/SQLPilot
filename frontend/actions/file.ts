"use server";
import { Types } from "mongoose";
import FileModel from "../models/File";
import { revalidatePath } from "next/cache";
import dbConnect from "@/lib/dbConnect";

export async function createFile(name: string, project_id: string) {
  try {
    await dbConnect();
    // Create a new document
    const newFile = {
      name,
      project_id: new Types.ObjectId(project_id),
    };

    // Use the create method instead of new + save
    const file = await FileModel.create(newFile);

    revalidatePath("/dashboard");
  } catch (error) {
    console.error("Error in createFile:", error);
    throw new Error(`Error creating file: ${error}`);
  }
}

export async function getFilesByProjectId(project_id: string) {
  try {
    await dbConnect();
    const files = await FileModel.find({
      project_id: new Types.ObjectId(project_id),
    }).exec();

    const res = files.map((file) => {
      return {
        name: file.name,
      };
    });
    return res;
  } catch (error) {
    console.error("Error in getFilesByProjectId:", error);
    throw new Error(`Error fetching files: ${error}`);
  }
}
