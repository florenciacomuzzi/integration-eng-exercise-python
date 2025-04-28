from bs4 import BeautifulSoup
import sys

def fix_nested_elements(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Find all elements with class 'path'
    path_elements = soup.find_all(class_='path')
    
    # Track elements that need to be fixed
    elements_to_fix = []
    
    for element in path_elements:
        # Check if this element has a parent with the same class
        parent = element.parent
        if parent and 'class' in parent.attrs and 'path' in parent.attrs['class']:
            elements_to_fix.append((parent, element))
    
    # Fix the nested elements
    for parent, child in elements_to_fix:
        # Move the child's content to the parent
        parent.string = child.string
        # Remove the child element
        child.decompose()
    
    return str(soup)

def main():
    if len(sys.argv) != 2:
        print("Usage: python fix_html_nesting.py <input_html_file>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    
    try:
        with open(input_file, 'r') as f:
            html_content = f.read()
        
        fixed_html = fix_nested_elements(html_content)
        
        # Write the fixed HTML back to the file
        with open(input_file, 'w') as f:
            f.write(fixed_html)
        
        print(f"Successfully fixed HTML in {input_file}")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 