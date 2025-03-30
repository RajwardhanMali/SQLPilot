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

    // Generate a session token
    const token = sign(
      { id: newUser._id, email: newUser.email, image },
      process.env.JWT_SECRET!,
      {
        expiresIn: "7d",
      }
    );

    const response = NextResponse.json({
      message: "User registered successfully",
    });
    response.cookies.set("authToken", token, { httpOnly: true, path: "/" });

    return response;
  } catch (error) {
    console.error(error);
    return NextResponse.json(
      { error: "Internal Server Error" },
      { status: 500 }
    );
  }
}
