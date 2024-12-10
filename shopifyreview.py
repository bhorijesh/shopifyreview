from bs4 import BeautifulSoup
import pandas as pd
import requests
from base.log_config import logger
import re
import time


def extract_reviews(soup):
    """
    Extracts reviews from the current page's BeautifulSoup object.
    Returns a list of extracted reviews.
    """
    try:
        main_container = soup.find(
            "div",
            class_="tw-col-span-full md:tw-col-span-9 lg:tw-col-span-8 lg:tw-pl-gutter--desktop"
        )
        if main_container:
            review_elements = main_container.find_all(
                "div",
                class_="tw-text-heading-xs tw-text-fg-primary tw-overflow-hidden tw-text-ellipsis tw-whitespace-nowrap"
            )
            reviews = []
            for review_element in review_elements:
                title = review_element.text.strip()
                location_element = review_element.find_next_sibling()
                location = location_element.text.strip() if location_element else 'Unknown'
                span_element = location_element.find_next_sibling() if location_element else None
                span = span_element.text.strip() if span_element else 'Unknown'

                reviews.append({
                    "Store": title,
                    "Location": location,
                    "Span": span,
                })

            return reviews
        else:
            logger.warning("Main container not found on this page")
            return None
    except Exception as e:
        logger.error(f"Error extracting reviews: {e}")
        return None


def extract_content(soup):
    """
    Extracts content from the current page's BeautifulSoup object.
    Returns a list of extracted content.
    """
    try:
        content_elements = soup.find_all("div", class_="tw-mb-xs md:tw-mb-sm")
        content = [content_element.text.strip() for content_element in content_elements]
        return content
    except Exception as e:
        logger.error(f"Error extracting content: {e}")
        return []


def time_rate_scrap(soup):
    """
    Extracts the 'rate' and 'time' from the soup.
    """
    try:
        rate_containers = soup.find_all('div', class_="tw-order-1 lg:tw-order-2 lg:tw-col-span-3 tw-overflow-x-auto")
        
        extracted_data = []
        for container in rate_containers:
            rate_element = container.find('div', class_="tw-flex tw-relative tw-space-x-0.5 tw-w-[88px] tw-h-md")
            rate_text = rate_element.get('aria-label', 'Unknown') if rate_element else 'Unknown'
            match = re.search(r'\d+', rate_text)
            rate = match.group() if match else 'Unknown'

            date_element = container.find('div', class_="tw-text-body-xs tw-text-fg-tertiary")
            date = date_element.text.strip() if date_element else 'Unknown'

            extracted_data.append({
                "Rate": rate,
                "Time": date,
            })
        
        return extracted_data
    except Exception as e:
        logger.error(f"Error extracting rate/time: {e}")
        return [{"Rate": 'Unknown', "Time": 'Unknown'}]


def detail_scrap(url):
    """
    Fetches and parses a page, then extracts reviews, content, and rates from it.
    """
    try:
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')
        logger.info(f"Successfully fetched page: {url}")
        
        extracted_reviews = extract_reviews(soup)
        extracted_content = extract_content(soup)
        extracted_time_rate = time_rate_scrap(soup)

        return {
            "reviews": extracted_reviews,
            "content": extracted_content,
            "time_rate": extracted_time_rate,
        }
    except Exception as e:  
        logger.error(f"Error scraping page {url}: {e}")
        return {}


def main():
    
    base_url = [
        "https://apps.shopify.com/google-customer-reviews-1/reviews?search_id=2e1fc3d9-d3c5-4254-acaf-bacae870ded4&page=",
        "https://apps.shopify.com/social-testimonial-slider/reviews?search_id=737d0deb-1c58-4a2f-bedf-b8e2e9edfd69&page=",
        "https://apps.shopify.com/reputon-testimonials/reviews?search_id=86905e06-08cb-4532-ab20-492267a980de&page=",
        "https://apps.shopify.com/product-reviews-by-appio/reviews?search_id=24b42449-d36f-4727-8365-7925219c6797&page=",
        "https://apps.shopify.com/revie/reviews?search_id=31a66c11-4f15-441f-b79d-1a9933b64247&page="
    ]
    
    combined_data = []
    for app_url in base_url:
        page_num = 1  # Start scraping at the first page
        
        while True:
            parts = app_url.split('apps.shopify.com/')[1].split('/reviews')[0]
            app_name = " ".join([word.capitalize() for word in parts.split('-')])
            # logger.info(app_name)
            url = f"{app_url}{page_num}"
            data = detail_scrap(url)
            
            if data and (data.get("reviews") or data.get("content") or data.get("time_rate")):
                reviews = data.get("reviews", [])
                content = data.get("content", [])
                time_rate_data = data.get("time_rate", [])

                # Combine the extracted data
                for review, cont, time_rate in zip(reviews, content, time_rate_data):
                    combined_data.append({
                        "Store": review.get("Store", ""),
                        "Location": review.get("Location", ""),
                        "Span": review.get("Span", ""),
                        "Rate": time_rate.get("Rate", "Unknown"),
                        "Time": time_rate.get("Time", "Unknown"),
                        "Review": cont,
                        "Application Name" : app_name
                    })

                logger.info(f"Extracted reviews and content from page {page_num}")
                page_num += 1  # Move to the next page
            else:
                # If no data is found, stop the loop
                logger.info(f"No more data found at page {page_num}. Ending scraping for {app_name}.")
                break
            time.sleep(5)  # Wait before scraping the next page

    # Save the combined data to a single CSV
    if combined_data:
        df = pd.DataFrame(combined_data)
        df.to_csv("shopify__reviews.csv", index=False)
        logger.info(f"Saved {len(combined_data)} combined records to 'reviews.csv'")
    else:
        logger.info("No reviews or content were extracted.")

if __name__ == "__main__":
    main()