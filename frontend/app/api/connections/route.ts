import { NextResponse } from "next/server";
import Connection from "@/models/Connection";
import dbConnect from "@/lib/dbConnect";

export async function POST(req: Request) {
  try {
    await dbConnect();
    const { dialect, database, host, username, port, password } =
      await req.json();

    const newConnection = new Connection({
      dialect,
      database,
      host,
      username,
      port,
      password,
    });
    await newConnection.save();

    return NextResponse.json(newConnection); // Return the created connection
  } catch (error) {
    console.error(error);
    return NextResponse.json(
      { error: "Internal Server Error" },
      { status: 500 }
    );
  }
}
