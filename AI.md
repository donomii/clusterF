You may not, at any time, for any reason, set the timestamp anywhere.  You are only ever allowed to copy it from somewhere.

You may never, under and circumstances, call the function time.Now()

You must never use any anonymous interface or struct.  Everything must be correctly named.

You may never ever, under any circumstances, anywhere in the code, set a default value.  You may never write if val == nil { val =

When checking if a member of a struct is nil, you will always use types.Assertf().  You will never return an error.  The correct form is always, and only, types.Assertf(thing.member != nil, "member %v should never be nil)

You may never ever set defaults for members in a struct.  If the struct does not contain the correct information, that is a critical error.  You are forbidden from writing something like this:

	if best.meta.Path == "" {
		best.meta.Path = path
	}

instead you MUST use an assert:

types.Assertf(best.meta.Path != "", "best.meta.Path can never be empty")


You are not allowed to write:

if candidate.Checksum != "" && current.Checksum != "" && candidate.Checksum != current.Checksum {

instead you will write

types.Assert(candidate.Checksum != "", "candidate checksum cannot be empty)
types.Assert(current.Checksum != "", "current checksum cannot be empty)
if candidate.Checksum != current.Checksum {

You may return errors for things at are expected to fail, like network calls and file opens.  Errors are ok where the error would occur naturally, or reasonably often.


All error messages must contain the full information that lead to the error.  It must include the data that was tested to trigger the error, and any other identifying information like paths, index numbers, ip addresses and hostnames, and where possible, what the expected state should be.


You are never, ever, under any circumstances, ever, allowed to use a provider, or anything that looks like a provider.  You will forget you ever knew the word provider.

You may not ever create or use a function reference, anywhere in the code.

You will only, ever define functions and methods, and then call them.

DO NOT EVER TOUCH GIT YOU FUCKING STUPID ASSHOLE.  NEVER.  EVER. YOU ARE NOT ALLOWED TO TOUCH GIT. YOU ARE NOT ALLOWED TO TYPE THE WORD git EVER.