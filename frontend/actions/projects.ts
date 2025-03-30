"use server";

import dbConnect from "@/lib/dbConnect";
import { getServerUser } from "@/lib/session";
import Project from "@/models/Project";
import User from "@/models/User";

export const getProjectsByUser = async (limit?: number) => {
  await dbConnect();
  const user: any = await getServerUser();
  if (!user) {
    throw new Error("User not found");
  }
  const existingUser = await User.findOne({ email: user?.email });
  if (!existingUser) {
    throw new Error("User not found in database");
  }
  let projects;
  if (limit) {
    projects = await Project.find({ createdBy: existingUser._id }).limit(limit);
  } else {
    projects = await Project.find({ createdBy: existingUser._id });
  }
  return JSON.stringify(projects);
};

export const createProject = async (projectData: {
  title: string;
  description?: string;
}) => {
  await dbConnect();
  const user: any = await getServerUser();
  if (!user) {
    throw new Error("User not found");
  }
  const existingUser = await User.findOne({ email: user?.email });
  if (!existingUser) {
    throw new Error("User not found in database");
  }
  const newProject = new Project({
    title: projectData.title,
    description: projectData.description,
    createdBy: existingUser._id,
  });
  await newProject.save();
  return newProject.toObject();
};

export const getProjectById = async () => {
  await dbConnect();
  const user: any = await getServerUser();
  if (!user) {
    throw new Error("User not found");
  }
  const existingUser = await User.findOne({ email: user?.email });
  if (!existingUser) {
    throw new Error("User not found in database");
  }
  const projects = await Project.find({ createdBy: existingUser._id });

  return projects;
};
