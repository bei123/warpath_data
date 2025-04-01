from PIL import Image, ImageDraw

def create_icon():
    # 创建一个 256x256 的图像
    size = 256
    image = Image.new('RGBA', (size, size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(image)
    
    # 绘制一个简单的圆形
    margin = 20
    draw.ellipse([margin, margin, size-margin, size-margin], 
                 fill=(66, 133, 244, 255))  # Google Blue
    
    # 绘制一个简单的"W"字母
    draw.text((size//2, size//2), "W", 
              fill=(255, 255, 255, 255), 
              anchor="mm",
              font=None,
              font_size=size//2)
    
    # 保存为ICO文件
    image.save('icon.ico', format='ICO')

if __name__ == '__main__':
    create_icon() 