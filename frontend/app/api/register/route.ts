import { NextResponse } from "next/server";
import bcrypt from "bcrypt";
import User from "@/models/User";
import dbConnect from "@/lib/dbConnect";
import { sign } from "jsonwebtoken";

export async function POST(req: Request) {
  try {
    await dbConnect();
    const { email, password, name } = await req.json();

    const existingUser = await User.findOne({ email });
    if (existingUser) {
      return NextResponse.json(
        { error: "User already exists" },
        { status: 400 }
      );
    }
    const image = `https://ui-avatars.com/api/?name=${name}&background=random`;
    const hashedPassword = await bcrypt.hash(password, 10);
    const newUser = new User({ email, password: hashedPassword, name, image });
    await newUser.save();
    return NextResponse.json({
      message: "User registered successfully",
    });
  } catch (error) {
    console.error(error);
    return NextResponse.json(
      { error: "Internal Server Error" },
      { status: 500 }
    );
  }
}
