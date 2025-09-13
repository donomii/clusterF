//go:build darwin

package main

/*
#cgo CFLAGS: -x objective-c -fobjc-arc -fmodules
#cgo LDFLAGS: -framework Cocoa -framework CoreImage -framework CoreGraphics
#import <Cocoa/Cocoa.h>
#import <CoreImage/CoreImage.h>

static void setAppEmojiIconSilhouette(const char* emoji, double size) {
  @autoreleasepool {
    if (NSApp == nil) {
      [NSApplication sharedApplication];
    }
    NSString *str = [NSString stringWithUTF8String:emoji];
    if (!str) { return; }
    CGFloat s = (CGFloat)size;
    NSDictionary *attrs = @{ NSFontAttributeName: [NSFont systemFontOfSize:s] };
    NSSize textSize = [str sizeWithAttributes:attrs];
    if (textSize.width < 1 || textSize.height < 1) { textSize = NSMakeSize(s, s); }

    // Render emoji to NSImage
    NSImage *img = [[NSImage alloc] initWithSize:textSize];
    [img lockFocus];
    [str drawAtPoint:NSZeroPoint withAttributes:attrs];
    [img unlockFocus];

    // Convert to CIImage
    NSData *tiff = [img TIFFRepresentation];
    if (!tiff) { [NSApp setApplicationIconImage:img]; return; }
    CIImage *ci = [CIImage imageWithData:tiff];

    // Mask luminance to alpha
    CIFilter *mask = [CIFilter filterWithName:@"CIMaskToAlpha"];
    [mask setValue:ci forKey:kCIInputImageKey];
    CIImage *alpha = mask.outputImage ?: ci;

    // Create solid color image (frog green)
    CIColor *green = [[CIColor alloc] initWithColor:[NSColor colorWithCalibratedRed:0.13 green:0.77 blue:0.34 alpha:1.0]]; // #22C55E
    CIFilter *colorGen = [CIFilter filterWithName:@"CIConstantColorGenerator" keysAndValues:kCIInputColorKey, green, nil];
    CIImage *solid = [colorGen valueForKey:kCIOutputImageKey];
    solid = [solid imageByCroppingToRect:[alpha extent]];

    // Composite solid color with alpha mask
    CIFilter *comp = [CIFilter filterWithName:@"CISourceInCompositing"];
    [comp setValue:solid forKey:kCIInputImageKey];
    [comp setValue:alpha forKey:kCIInputBackgroundImageKey];
    CIImage *result = comp.outputImage ?: alpha;

    // Create NSImage from CIImage
    CIContext *ctx = [CIContext contextWithOptions:nil];
    CGRect rect = [result extent];
    CGImageRef cg = [ctx createCGImage:result fromRect:rect];
    if (cg) {
      NSImage *final = [[NSImage alloc] initWithCGImage:cg size:rect.size];
      CGImageRelease(cg);
      [NSApp setApplicationIconImage:final];
    } else {
      [NSApp setApplicationIconImage:img];
    }
  }
}
*/
import "C"
import "unsafe"

// setDockEmojiIcon sets the macOS Dock icon to a rendered emoji.
func setDockEmojiIcon(emoji string, size float64) {
    ce := C.CString(emoji)
    defer C.free(unsafe.Pointer(ce))
    C.setAppEmojiIconSilhouette(ce, C.double(size))
}
