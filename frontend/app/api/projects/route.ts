import { NextResponse } from "next/server";
import Project from "@/models/Project";
import dbConnect from "@/lib/dbConnect";




export async function POST(req: Request) {
  try {
    await dbConnect();
    const { title, description, createdBy, connection } = await req.json();

    const newProject = new Project({
      title,
      description,
      createdBy,
      connection,
    });
    await newProject.save();

    return NextResponse.json(newProject);
  } catch (error) {
    console.error(error);
    return NextResponse.json(
      { error: "Internal Server Error" },
      { status: 500 }
    );
  }
}
